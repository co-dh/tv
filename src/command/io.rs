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

const MAX_PREVIEW_ROWS: usize = 100_000;  // preview row limit
const BG_CHUNK_ROWS: usize = 100_000;  // rows per background chunk

/// Load file command (CSV, Parquet, or gzipped CSV)
/// Supports glob patterns for parquet files (e.g., "data/*.parquet")
pub struct From {
    pub file_path: String,
}

impl Command for From {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let p = &self.file_path;

        // Check for glob pattern (contains * or ?)
        if p.contains('*') || p.contains('?') {
            return self.load_glob(app, p);
        }

        let path = Path::new(p);
        if !path.exists() { return Err(anyhow!("File not found: {}", p)); }

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
                Some("parquet") => convert_epoch_cols(self.load_parquet(path)?),
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

    fn to_str(&self) -> String { format!("from {}", self.file_path) }
}

impl From {
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

    /// Load parquet files matching glob pattern using lazy scan (preview only)
    fn load_glob(&self, app: &mut AppContext, pattern: &str) -> Result<()> {
        let args = ScanArgsParquet::default();
        let lf = LazyFrame::scan_parquet(pattern, args)
            .map_err(|e| anyhow!("Failed to scan parquet: {}", e))?;
        let df = lf.limit(MAX_PREVIEW_ROWS as u32).collect()
            .map_err(|e| anyhow!("Schema mismatch in parquet files: {}", e))?;
        if df.height() == 0 { return Err(anyhow!("No data found matching pattern")); }
        let df = convert_epoch_cols(df);
        app.msg(format!("Preview: {} rows, {} cols from {}", df.height(), df.width(), pattern));
        app.stack = crate::state::StateStack::init(ViewState::new(app.next_id(), pattern.to_string(), df, None));
        Ok(())
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
                return Ok((parse_csv_buf(buf, sep, 500)?, None));
            }
            buf.extend_from_slice(line.as_bytes());
        }

        // Parse preview
        let df = parse_csv_buf(buf, sep, 500)?;

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
        if let Ok(df) = parse_csv_buf(chunk_buf, sep, 100) {
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

/// Parse CSV buffer with separator (DRY helper)
fn parse_csv_buf(buf: Vec<u8>, sep: u8, schema_len: usize) -> Result<DataFrame> {
    CsvReadOptions::default()
        .with_has_header(true)
        .with_infer_schema_length(Some(schema_len))
        .map_parse_options(|o| o.with_separator(sep))
        .into_reader_with_file_handle(std::io::Cursor::new(buf))
        .finish()
        .map_err(|e| anyhow!("Failed to parse: {}", e))
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
/// TAQ values are 13-15 digits (HHMMSSNNNNNNNNN with leading zeros)
fn is_taq_time(v: i64) -> bool {
    if v < 0 { return false; }
    // TAQ time: must be 13-15 digit range (roughly 1e12 to 1e15)
    if v < 1_000_000_000_000 || v >= 1_000_000_000_000_000 { return false; }
    let s = format!("{:015}", v);
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

/// Convert integer/float columns with datetime-like names to datetime/time
fn convert_epoch_cols(df: DataFrame) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for c in df.get_columns() {
        let name = c.name().as_str();
        let is_numeric = c.dtype().is_integer() || c.dtype().is_float();
        if !is_datetime_name(name) || !is_numeric {
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

        // Try epoch conversion first (more common than TAQ)
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
            let raw = app.raw_save;
            app.msg(format!("Streaming {} to parquet{}...", gz, if raw { " (raw)" } else { "" }));
            app.bg_saver = Some(self.stream_gz_to_parquet(&gz, path, raw));
            return Ok(());
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
        let mut df = convert_epoch_cols(df.clone());  // convert int→time/datetime
        ParquetWriter::new(std::fs::File::create(path)?)
            .finish(&mut df)
            .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
        Ok(())
    }

    fn save_csv(&self, df: &DataFrame, path: &Path) -> Result<()> {
        CsvWriter::new(&mut std::fs::File::create(path)?)
            .finish(&mut df.clone())
            .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
        Ok(())
    }

    /// Stream entire gz file to parquet in chunks (runs in background)
    fn stream_gz_to_parquet(&self, gz_path: &str, out_path: &Path, raw: bool) -> Receiver<String> {
        let gz = gz_path.to_string();
        let out = out_path.to_path_buf();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            stream_gz_worker(&gz, &out, raw, tx);
        });

        rx
    }
}

/// Background worker for streaming gz to parquet (sends status via channel)
fn stream_gz_worker(gz_path: &str, out_path: &Path, raw: bool, tx: Sender<String>) {
    if let Err(e) = stream_gz_impl(gz_path, out_path, raw, &tx) {
        let _ = tx.send(format!("Save error: {}", e));
    }
}

const FIRST_CHUNK_ROWS: usize = 100_000;  // larger first chunk for schema detection

fn stream_gz_impl(gz_path: &str, out_path: &Path, raw: bool, tx: &Sender<String>) -> Result<()> {
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
    let col_count = header.split(sep as char).count();
    let header_bytes = header.as_bytes().to_vec();

    let prefix = out_path.file_stem().and_then(|s| s.to_str()).unwrap_or("out");
    let parent = out_path.parent().unwrap_or(Path::new("."));
    let mut file_idx = 1usize;
    let mut total_rows = 0usize;
    let mut final_schema: Option<Schema> = None;  // schema from first chunk (when not raw)

    loop {
        let mut chunk_buf = header_bytes.clone();
        let mut chunk_bytes = 0usize;
        let mut lines = 0usize;

        // First chunk is larger for better schema detection (when not raw)
        let max_lines = if !raw && file_idx == 1 { FIRST_CHUNK_ROWS } else { CHUNK_ROWS };

        while chunk_bytes < MAX_CHUNK_BYTES && lines < max_lines {
            let mut line = String::new();
            let n = reader.read_line(&mut line)?;
            if n == 0 { break; }
            chunk_buf.extend_from_slice(line.as_bytes());
            chunk_bytes += n;
            lines += 1;
        }

        if lines == 0 { break; }

        // Parse CSV with all columns as String first
        let cursor = std::io::Cursor::new(chunk_buf);
        let str_schema = Schema::from_iter((0..col_count).map(|i| {
            let name = header.split(sep as char).nth(i).unwrap_or("").trim();
            Field::new(name.into(), DataType::String)
        }));
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .with_schema(Some(Arc::new(str_schema)))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(cursor)
            .finish()
            .map_err(|e| anyhow!("Failed to parse chunk: {}", e))?;

        // raw mode: keep as String; otherwise detect/apply schema
        let df = if raw {
            df
        } else if let Some(ref schema) = final_schema {
            apply_schema(df, schema)
        } else {
            let df = convert_types(df);
            final_schema = Some(df.schema().to_owned());
            df
        };

        let chunk_path = parent.join(format!("{}_{:03}.parquet", prefix, file_idx));
        ParquetWriter::new(std::fs::File::create(&chunk_path)?)
            .finish(&mut df.clone())
            .map_err(|e| anyhow!("Failed to write {}: {}", chunk_path.display(), e))?;

        total_rows += df.height();
        let _ = tx.send(format!("Saved {} rows to {}", total_rows, chunk_path.display()));
        file_idx += 1;
    }

    let _ = child.wait();
    let _ = tx.send(format!("Done: {} files, {} rows", file_idx - 1, total_rows));
    Ok(())
}

/// Apply a fixed schema to dataframe (cast columns to match)
fn apply_schema(df: DataFrame, schema: &Schema) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for col in df.get_columns() {
        let name = col.name();
        if let Some(target_dtype) = schema.get(name) {
            if col.dtype() != target_dtype {
                // Time/Datetime: convert String→Int64 first, then apply TAQ/epoch logic
                if matches!(target_dtype, DataType::Time | DataType::Datetime(_, _)) && col.dtype() == &DataType::String {
                    if let Ok(i64_col) = col.cast(&DataType::Int64) {
                        let s = i64_col.as_materialized_series();
                        if let Ok(i64_ca) = s.i64() {
                            let v = i64_ca.iter().flatten().next();
                            if let Some(v) = v {
                                // TAQ time format
                                if is_taq_time(v) {
                                    let ns_ca: Int64Chunked = i64_ca.apply(|v| v.map(taq_to_ns));
                                    if let Ok(t) = ns_ca.into_series().cast(&DataType::Time) {
                                        cols.push(t.into_column());
                                        continue;
                                    }
                                }
                                // Epoch conversion
                                if let Some(unit) = epoch_unit(v) {
                                    let mult = if v.abs() < 10_000_000_000 { 1000i64 } else { 1 };
                                    let scaled = i64_ca.clone() * mult;
                                    if let Ok(dt) = scaled.into_series().cast(&DataType::Datetime(unit, None)) {
                                        cols.push(dt.into_column());
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                // Standard cast
                if let Ok(casted) = col.cast(target_dtype) {
                    cols.push(casted);
                    continue;
                }
            }
        }
        cols.push(col.clone());
    }
    DataFrame::new(cols).unwrap_or(df)
}

/// Check if string looks like a pure integer (no decimal, no scientific notation)
fn is_pure_int(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() { return true; }  // empty = null, ok
    let s = s.strip_prefix('-').unwrap_or(s);
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}

/// Check if string->i64->string round-trips (allows leading zeros)
fn int_roundtrip(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() { return true; }
    // Strip leading zeros for comparison (but keep at least one digit)
    let (neg, digits) = if let Some(rest) = s.strip_prefix('-') { (true, rest) } else { (false, s) };
    let stripped = digits.trim_start_matches('0');
    let canon = if stripped.is_empty() { "0".to_string() } else if neg { format!("-{}", stripped) } else { stripped.to_string() };
    s.parse::<i64>().map(|n| n.to_string() == canon).unwrap_or(false)
}

/// Check if string->f64->string round-trips (allowing trailing zeros)
fn float_roundtrip(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() { return true; }
    let Ok(f) = s.parse::<f64>() else { return false; };
    if !f.is_finite() { return false; }
    // Compare parsed values (handles trailing zeros, etc.)
    let back = format!("{}", f);
    back.parse::<f64>().map(|b| b == f).unwrap_or(false)
}

/// Convert string columns to appropriate types (lossless, conservative)
fn convert_types(df: DataFrame) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for col in df.get_columns() {
        let s = col.as_materialized_series();
        let Ok(str_ca) = s.str() else { cols.push(col.clone()); continue; };

        // Try i64: must be pure integers that round-trip exactly
        let all_int = str_ca.iter().all(|v| v.is_none() || (is_pure_int(v.unwrap()) && int_roundtrip(v.unwrap())));
        if all_int {
            if let Ok(int_s) = s.cast(&DataType::Int64) {
                cols.push(int_s.into_column());
                continue;
            }
        }

        // Try f64: must round-trip (allowing format differences)
        let all_float = str_ca.iter().all(|v| v.is_none() || float_roundtrip(v.unwrap()));
        if all_float {
            if let Ok(float_s) = s.cast(&DataType::Float64) {
                cols.push(float_s.into_column());
                continue;
            }
        }

        cols.push(col.clone());
    }
    convert_epoch_cols(DataFrame::new(cols).unwrap_or(df))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_types_mixed() {
        // Create df with all string columns, some convertible to int/float
        let df = DataFrame::new(vec![
            Column::new("bbo_ind".into(), &["O", "E", "A"]),
            Column::new("price".into(), &["10.5", "11", "12.5"]),  // 11 not 11.0
            Column::new("volume".into(), &["100", "200", "300"]),
        ]).unwrap();

        let df = convert_types(df);

        // bbo_ind should stay String (can't convert "O", "E", "A")
        assert_eq!(df.column("bbo_ind").unwrap().dtype(), &DataType::String);
        // price should be f64
        assert_eq!(df.column("price").unwrap().dtype(), &DataType::Float64);
        // volume should be i64
        assert_eq!(df.column("volume").unwrap().dtype(), &DataType::Int64);
    }

    #[test]
    fn test_is_pure_int() {
        assert!(is_pure_int("123"));
        assert!(is_pure_int("-456"));
        assert!(is_pure_int("0"));
        assert!(is_pure_int(""));  // empty = null
        assert!(!is_pure_int("12.3"));
        assert!(!is_pure_int("1e5"));
        assert!(!is_pure_int("abc"));
    }

    #[test]
    fn test_int_roundtrip() {
        assert!(int_roundtrip("123"));
        assert!(int_roundtrip("-456"));
        assert!(int_roundtrip("0123"));   // leading zeros allowed (for TAQ time)
        assert!(int_roundtrip("0"));
        assert!(int_roundtrip("-007"));
        assert!(!int_roundtrip("12.3"));  // decimals fail
    }

    #[test]
    fn test_float_roundtrip() {
        assert!(float_roundtrip("10.5"));
        assert!(float_roundtrip("-3.14"));
        assert!(float_roundtrip("0"));
        assert!(!float_roundtrip("abc"));
        assert!(!float_roundtrip("inf"));
    }

    #[test]
    fn test_detect_sep() {
        assert_eq!(detect_sep("a,b,c,d"), b',');
        assert_eq!(detect_sep("a|b|c|d"), b'|');
        assert_eq!(detect_sep("a\tb\tc\td"), b'\t');
        assert_eq!(detect_sep("a;b;c;d"), b';');
    }

    #[test]
    fn test_stream_gz_to_parquet() {
        use std::io::Write;
        use std::process::Command;

        // Create temp gz file with mixed types using system gzip
        let tmp = std::env::temp_dir();
        let csv_path = tmp.join("test_bbo.csv");
        let gz_path = tmp.join("test_bbo.csv.gz");
        let out_path = tmp.join("test_bbo.parquet");

        let csv = "National_BBO_Ind,price,volume\nO,10.5,100\nE,11,200\nA,12.5,300\n";
        let mut f = std::fs::File::create(&csv_path).unwrap();
        f.write_all(csv.as_bytes()).unwrap();

        // gzip the file
        let _ = std::fs::remove_file(&gz_path);
        let status = Command::new("gzip")
            .arg("-k")
            .arg(&csv_path)
            .status()
            .expect("gzip command failed");
        assert!(status.success());

        // Run the streaming save with a channel (not raw mode)
        let (tx, rx) = mpsc::channel();
        stream_gz_worker(gz_path.to_str().unwrap(), &out_path, false, tx);

        // Collect status messages
        let msgs: Vec<_> = rx.iter().collect();
        assert!(!msgs.is_empty());
        assert!(msgs.last().unwrap().contains("Done"));

        // Verify parquet was created with correct types
        let pq_path = tmp.join("test_bbo_001.parquet");
        assert!(pq_path.exists(), "parquet file should exist");

        let df = ParquetReader::new(std::fs::File::open(&pq_path).unwrap())
            .finish()
            .unwrap();

        assert_eq!(df.height(), 3);
        assert_eq!(df.column("National_BBO_Ind").unwrap().dtype(), &DataType::String);
        assert_eq!(df.column("price").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("volume").unwrap().dtype(), &DataType::Int64);

        // Cleanup
        let _ = std::fs::remove_file(csv_path);
        let _ = std::fs::remove_file(gz_path);
        let _ = std::fs::remove_file(pq_path);
    }
}
