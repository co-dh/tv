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
use std::io::{self, BufWriter, Write};

pub struct Renderer;

impl Renderer {
    /// Render the entire screen
    pub fn render(app: &mut AppContext) -> Result<()> {
        let (cols, rows) = terminal::size()?;

        let message = app.message.clone();

        // Use buffered writer to reduce flickering
        let mut stdout = BufWriter::new(io::stdout());

        if let Some(view) = app.current_view_mut() {
            Self::render_table(view, rows, cols, &mut stdout)?;
            Self::render_status_bar(view, &message, rows, cols, &mut stdout)?;
        } else {
            Self::render_empty_message(&message, rows, cols, &mut stdout)?;
        }

        stdout.flush()?;
        Ok(())
    }

    /// Render the table data
    fn render_table<W: Write>(view: &mut ViewState, rows: u16, cols: u16, writer: &mut W) -> Result<()> {
        let df = &view.dataframe;

        // Calculate column widths if needed
        if view.state.needs_width_recalc() {
            // Calculate row number width
            let row_num_width = df.height().to_string().len().max(3) as u16;
            let available_width = cols.saturating_sub(row_num_width + 1); // -1 for separator

            // Calculate base widths for all columns
            let mut widths: Vec<u16> = (0..df.width())
                .map(|col_idx| Self::calculate_column_width(df, col_idx, &view.state))
                .collect();

            // Find visible columns starting from c0
            let mut visible_widths = Vec::new();
            let mut visible_indices = Vec::new();

            for col_idx in view.state.c0..df.width() {
                let width = widths[col_idx];
                // Calculate space needed: sum of all widths + spaces between them
                let spaces_needed = if visible_widths.is_empty() { 0 } else { visible_widths.len() as u16 };
                let total_if_added = visible_widths.iter().sum::<u16>() + width + spaces_needed;

                if total_if_added > available_width && !visible_widths.is_empty() {
                    break;
                }
                visible_widths.push(width);
                visible_indices.push(col_idx);
            }

            // Distribute extra space among visible columns
            if !visible_widths.is_empty() {
                // Calculate space used: sum of widths + spaces between columns (not after last)
                let spaces = if visible_widths.len() > 1 { visible_widths.len() as u16 - 1 } else { 0 };
                let total_used = visible_widths.iter().sum::<u16>() + spaces;

                if total_used < available_width {
                    let extra_space = available_width - total_used;
                    let per_col = extra_space / visible_widths.len() as u16;
                    let remainder = extra_space % visible_widths.len() as u16;

                    for (i, &col_idx) in visible_indices.iter().enumerate() {
                        widths[col_idx] += per_col;
                        if i < remainder as usize {
                            widths[col_idx] += 1;
                        }
                    }
                }
            }

            view.state.col_widths = widths;
            view.state.widths_calc_row = view.state.cr;
        }

        let state = &view.state;

        if df.height() == 0 || df.width() == 0 {
            execute!(
                writer,
                cursor::MoveTo(0, 0),
                Print("(empty table)")
            )?;
            return Ok(());
        }

        // Calculate row number width
        let row_num_width = df.height().to_string().len().max(3) as u16;

        // Calculate visible area
        let visible_rows = (rows as usize).saturating_sub(2); // -2 for status bar and padding
        let end_row = (state.r0 + visible_rows).min(df.height());
        let data_cols = cols.saturating_sub(row_num_width + 1); // -1 for separator

        // Render column headers
        Self::render_headers(df, state, data_cols, row_num_width, writer)?;

        // Render data rows
        for row_idx in state.r0..end_row {
            let screen_row = (row_idx - state.r0 + 1) as u16; // +1 for header
            execute!(writer, cursor::MoveTo(0, screen_row))?;

            Self::render_row(df, row_idx, state, data_cols, row_num_width, writer)?;
        }

        // Clear any remaining lines
        for screen_row in (end_row - state.r0 + 1)..visible_rows {
            execute!(
                writer,
                cursor::MoveTo(0, screen_row as u16 + 1),
                terminal::Clear(terminal::ClearType::CurrentLine)
            )?;
        }

        Ok(())
    }

    /// Render column headers
    fn render_headers<W: Write>(df: &DataFrame, state: &TableState, term_cols: u16, row_num_width: u16, writer: &mut W) -> Result<()> {
        execute!(
            writer,
            cursor::MoveTo(0, 0),
            terminal::Clear(terminal::ClearType::CurrentLine)
        )?;

        // Render row number header
        let header = format!("{:>width$} ", "#", width = row_num_width as usize);
        execute!(writer, Print(&header))?;

        let mut col_offset = 0u16;
        for (col_idx, col_name) in df.get_column_names().iter().enumerate().skip(state.c0) {
            if col_offset >= term_cols {
                break;
            }

            let is_current = col_idx == state.cc;

            if is_current {
                execute!(
                    writer,
                    SetBackgroundColor(Color::Blue),
                    SetForegroundColor(Color::White)
                )?;
            }

            let col_width = Self::column_width(state, col_idx);
            let display = format!("{:width$}", col_name, width = col_width as usize);

            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current {
                execute!(writer, ResetColor)?;
            }

            execute!(writer, Print(" "))?;
            col_offset += col_width + 1;
        }

        Ok(())
    }

    /// Render a single data row
    fn render_row<W: Write>(df: &DataFrame, row_idx: usize, state: &TableState, term_cols: u16, row_num_width: u16, writer: &mut W) -> Result<()> {
        // Clear the line first
        execute!(writer, terminal::Clear(terminal::ClearType::CurrentLine))?;

        // Render row number
        let is_current_row = row_idx == state.cr;
        if is_current_row {
            execute!(writer, SetForegroundColor(Color::Yellow))?;
        }
        let row_num = format!("{:>width$} ", row_idx, width = row_num_width as usize);
        execute!(writer, Print(&row_num))?;
        if is_current_row {
            execute!(writer, ResetColor)?;
        }

        let mut col_offset = 0u16;

        for (col_idx, _col) in df.get_columns().iter().enumerate().skip(state.c0) {
            if col_offset >= term_cols {
                break;
            }

            let is_current_cell = row_idx == state.cr && col_idx == state.cc;
            let is_current_row = row_idx == state.cr;

            if is_current_cell {
                execute!(
                    writer,
                    SetBackgroundColor(Color::Yellow),
                    SetForegroundColor(Color::Black)
                )?;
            } else if is_current_row {
                execute!(writer, SetForegroundColor(Color::White))?;
            }

            let col_width = Self::column_width(state, col_idx);
            let value = Self::format_value(df, col_idx, row_idx);
            let display = format!("{:width$}", value, width = col_width as usize);

            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current_cell || is_current_row {
                execute!(writer, ResetColor)?;
            }

            execute!(writer, Print(" "))?;
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

    /// Get cached column width or default
    fn column_width(state: &TableState, col_idx: usize) -> u16 {
        state.col_widths.get(col_idx).copied().unwrap_or(10)
    }

    /// Calculate column width by sampling data around current row
    fn calculate_column_width(df: &DataFrame, col_idx: usize, state: &TableState) -> u16 {
        const MAX_WIDTH: usize = 30;
        const MIN_WIDTH: usize = 3;

        let col_name = &df.get_column_names()[col_idx];
        let mut max_width = col_name.len();

        // Sample 2-3 pages around current row for performance
        let page_size = state.viewport.0.saturating_sub(2) as usize;
        let sample_size = page_size * 3; // 3 pages

        let start_row = state.cr.saturating_sub(sample_size / 2);
        let end_row = (start_row + sample_size).min(df.height());

        // Check widths in the sample
        for row_idx in start_row..end_row {
            let value = Self::format_value(df, col_idx, row_idx);
            max_width = max_width.max(value.len());

            // Early exit if we hit max width
            if max_width >= MAX_WIDTH {
                break;
            }
        }

        max_width.max(MIN_WIDTH).min(MAX_WIDTH) as u16
    }

    /// Render status bar at the bottom
    fn render_status_bar<W: Write>(view: &ViewState, message: &str, rows: u16, cols: u16, writer: &mut W) -> Result<()> {
        let status_row = rows - 1;
        execute!(writer, cursor::MoveTo(0, status_row))?;

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
            writer,
            SetBackgroundColor(Color::DarkGrey),
            SetForegroundColor(Color::White),
            Print(&status[..status.len().min(cols as usize)]),
            ResetColor
        )?;

        Ok(())
    }

    /// Render message when no table is loaded
    fn render_empty_message<W: Write>(message: &str, rows: u16, _cols: u16, writer: &mut W) -> Result<()> {
        execute!(
            writer,
            cursor::MoveTo(0, rows / 2),
            Print(message)
        )?;
        Ok(())
    }
}
