//! File I/O commands (load/save CSV, Parquet, gz)
pub mod convert;

use crate::app::AppContext;
use crate::backend::{gz, Backend, Polars};
use crate::command::Command;
use anyhow::Result;
use convert::convert_epoch_cols;
use std::path::Path;

/// Load file command (CSV, Parquet, or gzipped CSV)
pub struct From {
    pub file_path: String,
}

impl Command for From {
    /// Load file: dispatch to gz or Polars backend
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let p = &self.file_path;
        let id = app.next_id();

        // Dispatch: .gz -> gz backend, else -> Polars backend
        let is_gz = Path::new(p).file_name().and_then(|s| s.to_str()).map(|s| s.ends_with(".gz")).unwrap_or(false);
        let result = if is_gz { gz::load(p, id) } else { Polars.load(p, id) }?;

        app.stack.push(result.view);
        app.bg_loader = result.bg_loader;
        Ok(())
    }

    fn to_str(&self) -> String { format!("from {}", self.file_path) }
}

/// Save file command (Parquet or CSV)
pub struct Save {
    pub file_path: String,
}

impl Command for Save {
    /// Save view to file: dispatch to backend.save or streaming gz
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let path = Path::new(&self.file_path);
        let is_parquet = !matches!(path.extension().and_then(|s| s.to_str()), Some("csv"));

        // Streaming save for gz source -> parquet (re-reads from disk)
        if is_parquet && view.gz_source.is_some() {
            let gz = view.gz_source.clone().unwrap();
            let raw = app.raw_save;
            app.msg(format!("Streaming {} to parquet{}...", gz, if raw { " (raw)" } else { "" }));
            app.bg_saver = Some(gz::stream_to_parquet(&gz, path, raw));
            return Ok(());
        }

        // Normal save via backend
        let df = if is_parquet { convert_epoch_cols(view.dataframe.clone()) } else { view.dataframe.clone() };
        view.backend().save(&df, path)
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}
