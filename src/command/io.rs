use crate::app::AppContext;
use crate::command::Command;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::path::Path;

/// Load file command (CSV or Parquet)
pub struct Load {
    pub file_path: String,
}

impl Command for Load {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let path = Path::new(&self.file_path);

        if !path.exists() {
            return Err(anyhow!("File not found: {}", self.file_path));
        }

        // Determine file type by extension
        let df = match path.extension().and_then(|s| s.to_str()) {
            Some("csv") => self.load_csv(path)?,
            Some("parquet") => self.load_parquet(path)?,
            Some(ext) => {
                return Err(anyhow!("Unsupported file format: {}", ext));
            }
            None => {
                return Err(anyhow!("Could not determine file type"));
            }
        };

        if df.height() == 0 {
            return Err(anyhow!("File is empty"));
        }

        // Replace the current stack with new view
        app.stack = crate::state::StateStack::init(ViewState::new(
            app.next_id(),
            self.file_path.clone(),
            df,
            Some(self.file_path.clone()),
        ));

        app.msg(format!(
            "Loaded {} ({} rows, {} cols)",
            self.file_path,
            app.view().unwrap().rows(),
            app.view().unwrap().cols()
        ));

        Ok(())
    }

    fn to_str(&self) -> String { format!("load {}", self.file_path) }
    fn record(&self) -> bool { false }  // don't record load in history
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
}

/// Save file command (Parquet)
pub struct Save {
    pub file_path: String,
}

impl Command for Save {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app.req()?;

        let path = Path::new(&self.file_path);

        // Determine format by extension (default to parquet)
        match path.extension().and_then(|s| s.to_str()) {
            Some("parquet") | None => self.save_parquet(&view.dataframe, path)?,
            Some("csv") => self.save_csv(&view.dataframe, path)?,
            Some(ext) => {
                return Err(anyhow!("Unsupported save format: {}", ext));
            }
        }

        app.msg(format!("Saved to {}", self.file_path));
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
}
