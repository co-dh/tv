//! File I/O commands (load/save) - uses DuckDB backend

use crate::app::AppContext;
use crate::command::Command;
use crate::data::backend;
use crate::state::ViewState;
use anyhow::{anyhow, Result};

/// Load file command (CSV, Parquet, or gzipped CSV)
pub struct From { pub file_path: String }

impl Command for From {
    /// Load file - lazy, data fetched on render via DuckDB
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let p = &self.file_path;
        let id = app.next_id();
        let basename = std::path::Path::new(p).file_name().and_then(|s| s.to_str()).unwrap_or(p);
        app.stack.push(ViewState::build(id, basename).path(p));
        app.msg(format!("Loaded {}", basename));
        Ok(())
    }

    fn to_str(&self) -> String { format!("from {}", self.file_path) }
}

/// Save file command (parquet/csv via backend)
pub struct Save { pub file_path: String }

impl Command for Save {
    /// Save view to file (parquet/csv via DuckDB, or in-memory CSV fallback)
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let v = app.req()?;
        let out = &self.file_path;

        // Check extension
        let is_pq = out.ends_with(".parquet") || out.ends_with(".pq");
        let is_csv = out.ends_with(".csv");
        if !is_pq && !is_csv { return Err(anyhow!("Only .parquet/.csv supported")); }

        // For file-backed views, use backend to save via PRQL
        if let Some(path_in) = &v.path {
            if backend::save(&v.prql, path_in, out) {
                app.msg(format!("Saved {}", out));
                return Ok(());
            }
        }

        // Fallback: in-memory table to CSV only
        if is_pq { return Err(anyhow!("Parquet save requires file-backed view")); }
        use std::io::Write;
        let mut f = std::fs::File::create(out)?;
        let cols = v.data.col_names();
        writeln!(f, "{}", cols.join(","))?;
        for r in 0..v.data.rows() {
            let row: Vec<String> = (0..cols.len()).map(|c| v.data.cell(r, c).format(10)).collect();
            writeln!(f, "{}", row.join(","))?;
        }
        app.msg(format!("Saved {}", out));
        Ok(())
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}
