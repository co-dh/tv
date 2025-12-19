//! File I/O commands (load/save) - uses plugin interface

use crate::app::AppContext;
use crate::command::Command;
use crate::dynload;
use crate::state::ViewState;
use anyhow::{anyhow, Result};
use std::path::Path;

/// Load file command (CSV, Parquet, or gzipped CSV)
pub struct From { pub file_path: String }

impl Command for From {
    /// Load file via plugin
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let p = &self.file_path;
        let id = app.next_id();

        // Use plugin to load/query file
        let plugin = dynload::get().ok_or_else(|| anyhow!("polars plugin not loaded"))?;

        // Get filename for display
        let name = Path::new(p).file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(p)
            .to_string();

        // Check if parquet for lazy loading
        let is_pq = p.ends_with(".parquet") || p.ends_with(".pq");
        let view = if is_pq {
            // Parquet: get count and schema, data fetched lazily
            let rows = plugin.count(p);
            let cols = plugin.schema(p);
            ViewState::new_parquet(id, &name, p, rows, cols)
        } else {
            // CSV/other: fetch all rows into memory
            let t = plugin.fetch(p, 0, 1_000_000).ok_or_else(|| anyhow!("Failed to load: {}", p))?;
            let table = dynload::to_box_table(&t);
            ViewState::new(id, &name, table, Some(p.to_string()))
        };

        app.stack.push(view);
        app.msg(format!("Loaded {}", name));
        Ok(())
    }

    fn to_str(&self) -> String { format!("from {}", self.file_path) }
}

/// Save file command (Parquet or CSV)
pub struct Save { pub file_path: String }

impl Command for Save {
    /// Save view to file - placeholder
    fn exec(&mut self, _app: &mut AppContext) -> Result<()> {
        // TODO: implement via plugin save method
        Err(anyhow!("Save not yet implemented - use plugin"))
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}
