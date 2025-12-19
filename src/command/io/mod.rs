//! File I/O commands (load/save) - uses plugin interface

use crate::app::AppContext;
use crate::command::Command;
use crate::data::dynload;
use crate::data::table::{Cell, Table};
use crate::state::ViewState;
use crate::util::pure;
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
            // Parquet: get count and schema, data fetched lazily via PRQL
            let count_sql = pure::compile_prql("from df | aggregate {n = count this}").ok_or_else(|| anyhow!("prql compile failed"))?;
            let rows = plugin.query(&count_sql, p)
                .and_then(|t| if t.rows() > 0 { Some(t.cell(0, 0)) } else { None })
                .and_then(|c| if let Cell::Int(n) = c { Some(n as usize) } else { None })
                .unwrap_or(0);
            let schema_sql = pure::compile_prql("from df | take 1").ok_or_else(|| anyhow!("prql compile failed"))?;
            let cols = plugin.query(&schema_sql, p).map(|t| t.col_names()).unwrap_or_default();
            ViewState::new_parquet(id, &name, p, rows, cols)
        } else {
            // CSV/other: fetch all rows into memory via PRQL
            let sql = pure::compile_prql("from df | take 1000000").ok_or_else(|| anyhow!("prql compile failed"))?;
            let t = plugin.query(&sql, p).ok_or_else(|| anyhow!("Failed to load: {}", p))?;
            let table = dynload::to_box_table(&t);
            ViewState::new(id, &name, table, Some(p.to_string()))
        };

        app.stack.push(view);
        app.msg(format!("Loaded {}", name));
        Ok(())
    }

    fn to_str(&self) -> String { format!("from {}", self.file_path) }
}

/// Save file command (CSV only for now)
pub struct Save { pub file_path: String }

impl Command for Save {
    /// Save view to CSV file
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        use std::io::Write;
        let v = app.req()?;
        let path = std::path::Path::new(&self.file_path);

        // Only support CSV for now
        if !self.file_path.ends_with(".csv") {
            return Err(anyhow!("Only .csv save supported"));
        }

        let mut f = std::fs::File::create(path)?;
        // Header
        let cols = v.data.col_names();
        writeln!(f, "{}", cols.join(","))?;
        // Data
        for r in 0..v.data.rows() {
            let row: Vec<String> = (0..cols.len()).map(|c| {
                let cell = v.data.cell(r, c);
                cell.format(10)  // CSV format
            }).collect();
            writeln!(f, "{}", row.join(","))?;
        }
        Ok(())
    }

    fn to_str(&self) -> String { format!("save {}", self.file_path) }
}
