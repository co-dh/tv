use crate::app::AppContext;
use crate::state::{TableState, ViewState};
use anyhow::Result;
use crossterm::{
    cursor,
    execute,
    style::{Color, Print, ResetColor, SetBackgroundColor, SetForegroundColor},
    terminal,
};
use polars::prelude::*;
use std::io::{self, Write};

pub struct Renderer;

impl Renderer {
    /// Render the entire screen
    pub fn render(app: &AppContext) -> Result<()> {
        let (rows, cols) = terminal::size()?;

        // Clear screen
        execute!(
            io::stdout(),
            terminal::Clear(terminal::ClearType::All),
            cursor::MoveTo(0, 0)
        )?;

        if let Some(view) = app.current_view() {
            Self::render_table(view, rows, cols)?;
            Self::render_status_bar(view, &app.message, rows, cols)?;
        } else {
            Self::render_empty_message(&app.message, rows, cols)?;
        }

        io::stdout().flush()?;
        Ok(())
    }

    /// Render the table data
    fn render_table(view: &ViewState, rows: u16, cols: u16) -> Result<()> {
        let df = &view.dataframe;
        let state = &view.state;

        if df.height() == 0 || df.width() == 0 {
            execute!(
                io::stdout(),
                cursor::MoveTo(0, 0),
                Print("(empty table)")
            )?;
            return Ok(());
        }

        // Calculate visible area
        let visible_rows = (rows as usize).saturating_sub(2); // -2 for status bar and padding
        let end_row = (state.r0 + visible_rows).min(df.height());

        // Render column headers
        Self::render_headers(df, state, cols)?;

        // Render data rows
        for row_idx in state.r0..end_row {
            let screen_row = (row_idx - state.r0 + 1) as u16; // +1 for header
            execute!(io::stdout(), cursor::MoveTo(0, screen_row))?;

            Self::render_row(df, row_idx, state, cols)?;
        }

        Ok(())
    }

    /// Render column headers
    fn render_headers(df: &DataFrame, state: &TableState, term_cols: u16) -> Result<()> {
        execute!(io::stdout(), cursor::MoveTo(0, 0))?;

        let mut col_offset = 0u16;
        for (col_idx, col_name) in df.get_column_names().iter().enumerate() {
            if col_offset >= term_cols {
                break;
            }

            let is_current = col_idx == state.cc;

            if is_current {
                execute!(
                    io::stdout(),
                    SetBackgroundColor(Color::Blue),
                    SetForegroundColor(Color::White)
                )?;
            }

            let col_width = Self::column_width(df, col_idx, term_cols);
            let display = format!("{:width$}", col_name, width = col_width as usize);

            execute!(io::stdout(), Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current {
                execute!(io::stdout(), ResetColor)?;
            }

            execute!(io::stdout(), Print(" "))?;
            col_offset += col_width + 1;
        }

        Ok(())
    }

    /// Render a single data row
    fn render_row(df: &DataFrame, row_idx: usize, state: &TableState, term_cols: u16) -> Result<()> {
        let mut col_offset = 0u16;

        for (col_idx, _col) in df.get_columns().iter().enumerate() {
            if col_offset >= term_cols {
                break;
            }

            let is_current_cell = row_idx == state.cr && col_idx == state.cc;
            let is_current_row = row_idx == state.cr;

            if is_current_cell {
                execute!(
                    io::stdout(),
                    SetBackgroundColor(Color::Yellow),
                    SetForegroundColor(Color::Black)
                )?;
            } else if is_current_row {
                execute!(io::stdout(), SetForegroundColor(Color::White))?;
            }

            let col_width = Self::column_width(df, col_idx, term_cols);
            let value = Self::format_value(df, col_idx, row_idx);
            let display = format!("{:width$}", value, width = col_width as usize);

            execute!(io::stdout(), Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current_cell || is_current_row {
                execute!(io::stdout(), ResetColor)?;
            }

            execute!(io::stdout(), Print(" "))?;
            col_offset += col_width + 1;
        }

        Ok(())
    }

    /// Format a single cell value
    fn format_value(df: &DataFrame, col_idx: usize, row_idx: usize) -> String {
        let col = df.get_columns()[col_idx].as_materialized_series();
        match col.dtype() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                col.get(row_idx).map(|v| v.to_string()).unwrap_or_else(|_| "null".to_string())
            }
            DataType::Float32 | DataType::Float64 => {
                col.get(row_idx).map(|v| v.to_string()).unwrap_or_else(|_| "null".to_string())
            }
            DataType::String => {
                col.str()
                    .ok()
                    .and_then(|s| s.get(row_idx))
                    .unwrap_or("null")
                    .to_string()
            }
            DataType::Boolean => {
                col.get(row_idx).map(|v| v.to_string()).unwrap_or_else(|_| "null".to_string())
            }
            _ => col.get(row_idx).map(|v| v.to_string()).unwrap_or_else(|_| "null".to_string()),
        }
    }

    /// Calculate column width
    fn column_width(df: &DataFrame, col_idx: usize, term_cols: u16) -> u16 {
        let col_name = &df.get_column_names()[col_idx];
        let base_width = col_name.len().max(10).min(30);
        base_width.min(term_cols as usize / df.width().max(1)) as u16
    }

    /// Render status bar at the bottom
    fn render_status_bar(view: &ViewState, message: &str, rows: u16, cols: u16) -> Result<()> {
        let status_row = rows - 1;
        execute!(io::stdout(), cursor::MoveTo(0, status_row))?;

        let status = if !message.is_empty() {
            message.to_string()
        } else {
            format!(
                "{}  Row {}/{} Col {}/{}  {}",
                view.filename.as_deref().unwrap_or("(no file)"),
                view.state.cr + 1,
                view.row_count(),
                view.state.cc + 1,
                view.col_count(),
                view.history_string()
            )
        };

        execute!(
            io::stdout(),
            SetBackgroundColor(Color::DarkGrey),
            SetForegroundColor(Color::White),
            Print(&status[..status.len().min(cols as usize)]),
            ResetColor
        )?;

        Ok(())
    }

    /// Render message when no table is loaded
    fn render_empty_message(message: &str, rows: u16, _cols: u16) -> Result<()> {
        execute!(
            io::stdout(),
            cursor::MoveTo(0, rows / 2),
            Print(message)
        )?;
        Ok(())
    }
}
