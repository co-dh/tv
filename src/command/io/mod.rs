//! File I/O commands (load/save CSV, Parquet, gz, DuckDB)
pub mod convert;
pub mod csv;
pub mod duckdb;
pub mod gz;
pub mod parquet;

use crate::app::AppContext;
use crate::command::Command;
use crate::os;
use crate::state::ViewState;
use crate::theme::load_config_value;
use anyhow::{anyhow, Result};
use convert::convert_epoch_cols;
use std::path::Path;

const MAX_PREVIEW_ROWS: usize = 100_000;

/// Load file command (CSV, Parquet, or gzipped CSV)
pub struct From {
    pub file_path: String,
}

impl Command for From {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let p = &self.file_path;

        // DuckDB SQL query: sql:SELECT * FROM 'file.parquet'
        if let Some(sql) = p.strip_prefix("sql:") {
            let df = duckdb::query(sql)?;
            if df.height() == 0 { return Err(anyhow!("Empty result")); }
            let df = convert_epoch_cols(df);
            let name = if sql.len() > 30 { format!("{}...", &sql[..30]) } else { sql.to_string() };
            let id = app.next_id();
            app.stack.push(ViewState::new(id, name, df, None));
            return Ok(());
        }

        // Glob pattern for parquet
        if p.contains('*') || p.contains('?') {
            let df = parquet::load_glob(p, MAX_PREVIEW_ROWS as u32)?;
            if df.height() == 0 { return Err(anyhow!("No data found matching pattern")); }
            let df = convert_epoch_cols(df);
            app.msg(format!("Preview: {} rows, {} cols from {}", df.height(), df.width(), p));
            app.stack = crate::state::StateStack::init(ViewState::new(app.next_id(), p.to_string(), df, None));
            return Ok(());
        }

        let path = Path::new(p);
        if !path.exists() { return Err(anyhow!("File not found: {}", p)); }

        let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let is_gz = fname.ends_with(".gz");

        if is_gz {
            // Get memory limit from config
            let mem_pct: u64 = load_config_value("gz_mem_pct").and_then(|s| s.parse().ok()).unwrap_or(10);
            let mem_limit = os::mem_total() * mem_pct / 100;

            let (df, bg_rx) = gz::load_streaming(path, mem_limit)?;
            let df = convert_epoch_cols(df);
            if df.height() == 0 { return Err(anyhow!("File is empty")); }
            let id = app.next_id();
            // partial = true if there's background loading (may hit mem limit)
            let partial = bg_rx.is_some();
            app.stack.push(ViewState::new_gz(
                id, self.file_path.clone(), df,
                Some(self.file_path.clone()), self.file_path.clone(), partial,
            ));
            app.bg_loader = bg_rx;
        } else {
            let df = match path.extension().and_then(|s| s.to_str()) {
                Some("csv") => convert_epoch_cols(csv::load(path)?),
                Some("parquet") => convert_epoch_cols(parquet::load(path)?),
                Some(ext) => return Err(anyhow!("Unsupported file format: {}", ext)),
                None => return Err(anyhow!("Could not determine file type")),
            };
            if df.height() == 0 { return Err(anyhow!("File is empty")); }
            let id = app.next_id();
            app.stack.push(ViewState::new(id, self.file_path.clone(), df, Some(self.file_path.clone())));
        }
        Ok(())
    }

    fn to_str(&self) -> String { format!("from {}", self.file_path) }
}

/// Save file command (Parquet or CSV)
pub struct Save {
    pub file_path: String,
}

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
            app.bg_saver = Some(gz::stream_to_parquet(&gz, path, raw));
            return Ok(());
        }

        // Normal save
        match path.extension().and_then(|s| s.to_str()) {
            Some("parquet") | None => {
                let df = convert_epoch_cols(view.dataframe.clone());
                parquet::save(&df, path)?;
            }
            Some("csv") => csv::save(&view.dataframe, path)?,
            Some(ext) => return Err(anyhow!("Unsupported save format: {}", ext)),
        }
        Ok(())
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}
