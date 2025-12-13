use crate::app::AppContext;
use crate::command::Command;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command as Cmd, Stdio};
use std::sync::Arc;

const MAX_PREVIEW_ROWS: usize = 1000;  // preview row limit

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
            let df = self.load_csv_gz(path)?;
            if df.height() == 0 { return Err(anyhow!("File is empty")); }
            app.stack = crate::state::StateStack::init(ViewState::new_gz(
                app.next_id(), self.file_path.clone(), df,
                Some(self.file_path.clone()), self.file_path.clone(),
            ));
        } else {
            let df = match path.extension().and_then(|s| s.to_str()) {
                Some("csv") => self.load_csv(path)?,
                Some("parquet") => self.load_parquet(path)?,
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

    /// Load first N rows from gzipped CSV using zcat, auto-detect delimiter
    fn load_csv_gz(&self, path: &Path) -> Result<DataFrame> {
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

        // Read up to MAX_PREVIEW_ROWS lines
        let mut buf = header.into_bytes();
        for _ in 0..MAX_PREVIEW_ROWS {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 { break; }
            buf.extend_from_slice(line.as_bytes());
        }

        let _ = child.kill();
        let _ = child.wait();

        // Parse with detected separator
        let cursor = std::io::Cursor::new(buf);
        CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(500))
            .map_parse_options(|o| o.with_separator(sep))
            .into_reader_with_file_handle(cursor)
            .finish()
            .map_err(|e| anyhow!("Failed to parse: {}", e))
    }
}

/// Detect separator by counting occurrences in header line
fn detect_sep(line: &str) -> u8 {
    let seps = [(b'|', line.matches('|').count()),
                (b'\t', line.matches('\t').count()),
                (b',', line.matches(',').count()),
                (b';', line.matches(';').count())];
    seps.into_iter().max_by_key(|&(_, n)| n).map(|(c, _)| c).unwrap_or(b',')
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
    fn stream_gz_to_parquet(&self, gz_path: &str, schema: &Schema, out_path: &Path) -> Result<()> {
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

            let cursor = std::io::Cursor::new(chunk_buf);
            let df = CsvReadOptions::default()
                .with_has_header(true)
                .with_schema(Some(Arc::new(schema.clone())))
                .map_parse_options(|o| o.with_separator(sep))
                .into_reader_with_file_handle(cursor)
                .finish()
                .map_err(|e| anyhow!("Failed to parse chunk: {}", e))?;

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
