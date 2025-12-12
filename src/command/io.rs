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
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
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

        // Create new view with the loaded data
        let view = ViewState::new(
            String::from("main"),
            df,
            Some(self.file_path.clone()),
        );

        // Replace the current stack with new view
        app.stack = crate::state::StateStack::with_initial(view);

        app.set_message(format!(
            "Loaded {} ({} rows, {} cols)",
            self.file_path,
            app.current_view().unwrap().row_count(),
            app.current_view().unwrap().col_count()
        ));

        Ok(())
    }

    fn to_command_string(&self) -> String {
        format!("load {}", self.file_path)
    }

    fn should_record(&self) -> bool {
        // Don't record load commands in history
        false
    }
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
        let file = std::fs::File::open(path)?;
        ParquetReader::new(file)
            .finish()
            .map_err(|e| anyhow!("Failed to read Parquet: {}", e))
    }
}

/// Save file command (Parquet)
pub struct Save {
    pub file_path: String,
}

impl Command for Save {
    fn execute(&mut self, app: &mut AppContext) -> Result<()> {
        let view = app
            .current_view()
            .ok_or_else(|| anyhow!("No table loaded"))?;

        let path = Path::new(&self.file_path);

        // Determine format by extension (default to parquet)
        match path.extension().and_then(|s| s.to_str()) {
            Some("parquet") | None => self.save_parquet(&view.dataframe, path)?,
            Some("csv") => self.save_csv(&view.dataframe, path)?,
            Some(ext) => {
                return Err(anyhow!("Unsupported save format: {}", ext));
            }
        }

        app.set_message(format!("Saved to {}", self.file_path));
        Ok(())
    }

    fn to_command_string(&self) -> String {
        format!("save {}", self.file_path)
    }
}

impl Save {
    fn save_parquet(&self, df: &DataFrame, path: &Path) -> Result<()> {
        let file = std::fs::File::create(path)?;
        ParquetWriter::new(file)
            .finish(&mut df.clone())
            .map_err(|e| anyhow!("Failed to write Parquet: {}", e))?;
        Ok(())
    }

    fn save_csv(&self, df: &DataFrame, path: &Path) -> Result<()> {
        let mut file = std::fs::File::create(path)?;
        CsvWriter::new(&mut file)
            .finish(&mut df.clone())
            .map_err(|e| anyhow!("Failed to write CSV: {}", e))?;
        Ok(())
    }
}
