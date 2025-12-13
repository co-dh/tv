use crate::app::AppContext;
use crate::command::Command;
use crate::os;
use crate::state::ViewState;
use crate::theme::load_config_value;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{ChildStdout, Command as Cmd, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

const MAX_PREVIEW_ROWS: usize = 1000;  // preview row limit
const BG_CHUNK_ROWS: usize = 100_000;  // rows per background chunk

/// Load file command (CSV, Parquet, or gzipped CSV)
pub struct Load {
    pub file_path: String,
}

impl Command for Load {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let path = Path::new(&self.file_path);
        if !path.exists() { return Err(anyhow!("File not found: {}", self.file_path)); }

        // Check for .gz extension (csv.gz, tsv.gz, or plain .gz)
        let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let is_gz = fname.ends_with(".gz");

        if is_gz {
            let (df, bg_rx) = self.load_csv_gz_streaming(path)?;
            let df = convert_epoch_cols(df);
            if df.height() == 0 { return Err(anyhow!("File is empty")); }
            app.stack = crate::state::StateStack::init(ViewState::new_gz(
                app.next_id(), self.file_path.clone(), df,
                Some(self.file_path.clone()), self.file_path.clone(),
            ));
            app.bg_loader = bg_rx;
        } else {
            let df = match path.extension().and_then(|s| s.to_str()) {
                Some("csv") => convert_epoch_cols(self.load_csv(path)?),
                Some("parquet") => self.load_parquet(path)?,  // parquet keeps types
                Some(ext) => return Err(anyhow!("Unsupported file format: {}", ext)),
                None => return Err(anyhow!("Could not determine file type")),
            };
            if df.height() == 0 { return Err(anyhow!("File is empty")); }
            app.stack = crate::state::StateStack::init(ViewState::new(
                app.next_id(), self.file_path.clone(), df, Some(self.file_path.clone()),
            ));
        }
        Ok(())
    }

    fn to_str(&self) -> String { format!("load {}", self.file_path) }
    fn record(&self) -> bool { false }
}

impl Load {
    fn load_csv(&self, path: &Path) -> Result<DataFrame> {
        CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(100))
            .try_into_reader_with_file_path(Some(path.to_path_buf()))?
            .finish()
            .map_err(|e| anyhow!("Failed to read CSV: {}", e))
    }

    fn load_parquet(&self, path: &Path) -> Result<DataFrame> {
        ParquetReader::new(std::fs::File::open(path)?)
            .finish()
            .map_err(|e| anyhow!("Failed to read Parquet: {}", e))
    }

    /// Load preview rows, spawn background thread to continue loading
    fn load_csv_gz_streaming(&self, path: &Path) -> Result<(DataFrame, Option<Receiver<DataFrame>>)> {
        let mut child = Cmd::new("zcat")
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
                // File fully read, no background needed
                let _ = child.wait();
                let cursor = std::io::Cursor::new(buf);
                let df = CsvReadOptions::default()
                    .with_has_header(true)
                    .with_infer_schema_length(Some(500))
                    .map_parse_options(|o| o.with_separator(sep))
                    .into_reader_with_file_handle(cursor)
                    .finish()
                    .map_err(|e| anyhow!("Failed to parse: {}", e))?;
                return Ok((df, None));
            }
            buf.extend_from_slice(line.as_bytes());
        }

        // Parse preview
        let cursor = std::io::Cursor::new(buf);
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(500))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(cursor)
            .finish()
            .map_err(|e| anyhow!("Failed to parse: {}", e))?;

        // Get memory limit from config
        let mem_pct: u64 = load_config_value("gz_mem_pct")
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        let mem_limit = os::mem_total() * mem_pct / 100;

        // Spawn background loader
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            bg_stream_csv(reader, header_bytes, sep, mem_limit, tx, child);
        });

        Ok((df, Some(rx)))
    }
}

/// Background thread: stream CSV chunks until memory limit
fn bg_stream_csv(
    mut reader: BufReader<ChildStdout>,
    header: Vec<u8>,
    sep: u8,
    mem_limit: u64,
    tx: Sender<DataFrame>,
    mut child: std::process::Child,
) {
    let mut total_bytes = 0u64;

    loop {
        let mut chunk_buf = header.clone();
        let mut lines = 0usize;

        // Read chunk
        while lines < BG_CHUNK_ROWS {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,  // EOF
                Ok(n) => {
                    chunk_buf.extend_from_slice(line.as_bytes());
                    total_bytes += n as u64;
                    lines += 1;
                }
                Err(_) => break,
            }
        }

        if lines == 0 { break; }  // no more data

        // Parse chunk
        let cursor = std::io::Cursor::new(chunk_buf);
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(100))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(cursor)
            .finish();

        if let Ok(df) = df {
            let df = convert_epoch_cols(df);
            if tx.send(df).is_err() { break; }  // receiver dropped
        }

        // Check memory limit (rough estimate: ~2x raw size for DataFrame)
        if total_bytes * 2 > mem_limit { break; }
    }

    let _ = child.kill();
    let _ = child.wait();
}

/// Detect separator by counting occurrences in header line
fn detect_sep(line: &str) -> u8 {
    let seps = [(b'|', line.matches('|').count()),
                (b'\t', line.matches('\t').count()),
                (b',', line.matches(',').count()),
                (b';', line.matches(';').count())];
    seps.into_iter().max_by_key(|&(_, n)| n).map(|(c, _)| c).unwrap_or(b',')
}

/// Check if column name looks like a datetime field
fn is_datetime_name(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("time") || n.contains("date") || n.contains("created") || n.contains("updated")
        || n.contains("_at") || n.contains("_ts") || n == "ts" || n == "dt"
}

/// Detect epoch unit from integer value: sec, ms, us, ns
fn epoch_unit(v: i64) -> Option<TimeUnit> {
    let abs = v.abs();
    if abs > 1_000_000_000_000_000_000 && abs < 3_000_000_000_000_000_000 { Some(TimeUnit::Nanoseconds) }
    else if abs > 1_000_000_000_000_000 && abs < 3_000_000_000_000_000 { Some(TimeUnit::Microseconds) }
    else if abs > 1_000_000_000_000 && abs < 3_000_000_000_000 { Some(TimeUnit::Milliseconds) }
    else if abs > 1_000_000_000 && abs < 3_000_000_000 { Some(TimeUnit::Milliseconds) }  // treat sec as ms*1000
    else { None }
}

/// Check if value looks like TAQ time format (HHMMSS + fractional ns)
/// e.g., 035900085993578 = 03:59:00.085993578
fn is_taq_time(v: i64) -> bool {
    if v < 0 { return false; }
    let s = format!("{:015}", v);  // pad to 15 digits
    let hh: u32 = s[0..2].parse().unwrap_or(99);
    let mm: u32 = s[2..4].parse().unwrap_or(99);
    let ss: u32 = s[4..6].parse().unwrap_or(99);
    hh < 24 && mm < 60 && ss < 60
}

/// Convert TAQ time format to nanoseconds since midnight
fn taq_to_ns(v: i64) -> i64 {
    let s = format!("{:015}", v);
    let hh: i64 = s[0..2].parse().unwrap_or(0);
    let mm: i64 = s[2..4].parse().unwrap_or(0);
    let ss: i64 = s[4..6].parse().unwrap_or(0);
    let frac: i64 = s[6..15].parse().unwrap_or(0);  // 9 digits of fractional ns
    (hh * 3600 + mm * 60 + ss) * 1_000_000_000 + frac
}

/// Convert integer columns with datetime-like names to datetime
fn convert_epoch_cols(df: DataFrame) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for c in df.get_columns() {
        let name = c.name().as_str();
        if !is_datetime_name(name) || !c.dtype().is_integer() {
            cols.push(c.clone());
            continue;
        }
        // Cast to i64 and sample first non-null value
        let s = c.as_materialized_series();
        let i64_s = match s.cast(&DataType::Int64) {
            Ok(s) => s,
            Err(_) => { cols.push(c.clone()); continue; }
        };
        let i64_ca = match i64_s.i64() {
            Ok(ca) => ca,
            Err(_) => { cols.push(c.clone()); continue; }
        };
        let sample = i64_ca.into_iter().flatten().next();
        let Some(v) = sample else { cols.push(c.clone()); continue; };

        // Try epoch conversion first
        if let Some(unit) = epoch_unit(v) {
            let multiplier = if v.abs() < 10_000_000_000 { 1000i64 } else { 1 };
            let scaled = i64_ca.clone() * multiplier;
            if let Ok(dt) = scaled.into_series().cast(&DataType::Datetime(unit, None)) {
                cols.push(dt.into_column());
                continue;
            }
        }

        // Try TAQ time format (HHMMSS + fractional ns)
        if is_taq_time(v) {
            let ns_ca: Int64Chunked = i64_ca.apply(|v| v.map(taq_to_ns));
            if let Ok(t) = ns_ca.into_series().cast(&DataType::Time) {
                cols.push(t.into_column());
                continue;
            }
        }

        cols.push(c.clone());
    }
    DataFrame::new(cols).unwrap_or(df)
}

/// Save file command (Parquet) - supports streaming from gz source
pub struct Save {
    pub file_path: String,
}

const CHUNK_ROWS: usize = 10_000_000;  // rows per chunk for streaming
const MAX_CHUNK_BYTES: usize = 1024 * 1024 * 1024;  // ~1GB per parquet file

impl Command for Save {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let path = Path::new(&self.file_path);
        let is_parquet = matches!(path.extension().and_then(|s| s.to_str()), Some("parquet") | None);

        // Streaming save if gz_source exists and saving to parquet
        if is_parquet && view.gz_source.is_some() {
            let gz = view.gz_source.clone().unwrap();
            let schema = view.dataframe.schema();
            app.msg(format!("Streaming {} to parquet...", gz));
            return self.stream_gz_to_parquet(&gz, &schema, path);
        }

        // Normal save
        match path.extension().and_then(|s| s.to_str()) {
            Some("parquet") | None => self.save_parquet(&view.dataframe, path)?,
            Some("csv") => self.save_csv(&view.dataframe, path)?,
            Some(ext) => return Err(anyhow!("Unsupported save format: {}", ext)),
        }
        Ok(())
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}

impl Save {
    fn save_parquet(&self, df: &DataFrame, path: &Path) -> Result<()> {
        ParquetWriter::new(std::fs::File::create(path)?)
            .finish(&mut df.clone())
            .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
        Ok(())
    }

    fn save_csv(&self, df: &DataFrame, path: &Path) -> Result<()> {
        CsvWriter::new(&mut std::fs::File::create(path)?)
            .finish(&mut df.clone())
            .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
        Ok(())
    }

    /// Stream entire gz file to parquet in chunks
    /// Note: schema param unused - we infer from CSV and convert epoch cols
    fn stream_gz_to_parquet(&self, gz_path: &str, _schema: &Schema, out_path: &Path) -> Result<()> {
        let mut child = Cmd::new("zcat")
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
        let header_bytes = header.as_bytes().to_vec();

        let prefix = out_path.file_stem().and_then(|s| s.to_str()).unwrap_or("out");
        let parent = out_path.parent().unwrap_or(Path::new("."));
        let mut file_idx = 1usize;
        let mut total_rows = 0usize;

        loop {
            let mut chunk_buf = header_bytes.clone();
            let mut chunk_bytes = 0usize;
            let mut lines = 0usize;

            while chunk_bytes < MAX_CHUNK_BYTES && lines < CHUNK_ROWS {
                let mut line = String::new();
                let n = reader.read_line(&mut line)?;
                if n == 0 { break; }
                chunk_buf.extend_from_slice(line.as_bytes());
                chunk_bytes += n;
                lines += 1;
            }

            if lines == 0 { break; }

            // Parse CSV without schema (infer types), then convert epoch columns
            let cursor = std::io::Cursor::new(chunk_buf);
            let df = CsvReadOptions::default()
                .with_has_header(true)
                .with_infer_schema_length(Some(500))
                .map_parse_options(|o| o.with_separator(sep))
                .into_reader_with_file_handle(cursor)
                .finish()
                .map_err(|e| anyhow!("Failed to parse chunk: {}", e))?;

            // Apply same epoch/TAQ conversion as preview load
            let df = convert_epoch_cols(df);

            let chunk_path = parent.join(format!("{}_{:03}.parquet", prefix, file_idx));
            ParquetWriter::new(std::fs::File::create(&chunk_path)?)
                .finish(&mut df.clone())
                .map_err(|e| anyhow!("Failed to write {}: {}", chunk_path.display(), e))?;

            total_rows += df.height();
            eprintln!("Wrote {} ({} rows, total {})", chunk_path.display(), df.height(), total_rows);
            file_idx += 1;
        }

        let _ = child.wait();
        eprintln!("Done: {} files, {} total rows", file_idx - 1, total_rows);
        Ok(())
    }
}
