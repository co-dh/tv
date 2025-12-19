//! File I/O commands (load/save CSV, Parquet, gz)
pub mod convert;

use crate::app::AppContext;
use crate::source::{gz, Source, Polars};
use crate::command::Command;
use anyhow::Result;
use convert::convert_epoch_cols;
use std::path::Path;

/// Load file command (CSV, Parquet, or gzipped CSV)
pub struct From {
    pub file_path: String,
}

impl Command for From {
    /// Load file: dispatch to gz or Polars source
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let p = &self.file_path;
        let id = app.next_id();

        // Dispatch: .gz -> gz source, else -> Polars source
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
    /// Save view to file: dispatch to source.save or streaming gz
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;
        let path = Path::new(&self.file_path);
        let is_parquet = !matches!(path.extension().and_then(|s| s.to_str()), Some("csv"));

        // Streaming save for gz source -> parquet (re-reads from disk)
        if let Some(gz) = view.gz_source.clone().filter(|_| is_parquet) {
            let raw = app.raw_save;
            app.msg(format!("Streaming {} to parquet{}...", gz, if raw { " (raw)" } else { "" }));
            app.bg_saver = Some(gz::stream_to_parquet(&gz, path, raw));
            return Ok(());
        }

        // Normal save via source
        let df = if is_parquet { convert_epoch_cols(view.dataframe.clone()) } else { view.dataframe.clone() };
        view.source().save(&df, path)
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}
