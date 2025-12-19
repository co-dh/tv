//! File I/O commands (load/save CSV, Parquet, gz)
pub mod convert;

use crate::app::AppContext;
use crate::source::{gz, Source, Polars};
use crate::state::ViewSource;
use crate::table::table_to_df;
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
    /// Save view to file: dispatch to backend.save or streaming gz
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let path = Path::new(&self.file_path);
        let is_parquet = !matches!(path.extension().and_then(|s| s.to_str()), Some("csv"));

        // Extract what we need from view before mutable operations
        let (gz_source, df_clone, is_pq) = {
            let view = app.req()?;
            let gz = if let ViewSource::Gz { path: ref p, .. } = view.source { Some(p.clone()) } else { None };
            let df = table_to_df(view.data.as_ref());
            let df = if is_parquet { convert_epoch_cols(df) } else { df };
            let pq = view.source.is_parquet();
            (gz, df, pq)
        };

        // Streaming save for gz source -> parquet (re-reads from disk)
        if let Some(ref gz_path) = gz_source {
            if is_parquet {
                let raw = app.raw_save;
                app.msg(format!("Streaming {} to parquet{}...", gz_path, if raw { " (raw)" } else { "" }));
                app.bg_saver = Some(gz::stream_to_parquet(gz_path, path, raw));
                return Ok(());
            }
        }

        // Normal save via backend
        if is_pq { Polars.save(&df_clone, path) }
        else { crate::source::Memory(&df_clone).save(&df_clone, path) }
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}
