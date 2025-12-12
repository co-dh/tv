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
            // Calculate base widths for all columns
            let widths: Vec<u16> = (0..df.width())
                .map(|col_idx| Self::calculate_column_width(df, col_idx, &view.state))
                .collect();

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

        // Calculate row number width (0 if not showing row numbers)
        let row_num_width = if view.show_row_numbers {
            df.height().to_string().len().max(3) as u16
        } else {
            0
        };
        let screen_width = cols.saturating_sub(if row_num_width > 0 { row_num_width + 1 } else { 0 }) as i32;

        // Calculate xs - x position for each column (qtv style)
        let mut xs: Vec<i32> = Vec::with_capacity(df.width() + 1);
        xs.push(0);
        for col_idx in 0..df.width() {
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as i32;
            let last = *xs.last().unwrap();
            xs.push(last + col_width + 1); // +1 for space between columns
        }

        // If cursor column right edge exceeds screen width, shift left
        let cursor_right = xs.get(state.cc + 1).copied().unwrap_or(0);
        if cursor_right > screen_width {
            // Find shift: first column whose position > (cursor_right - screen_width)
            let threshold = cursor_right - screen_width;
            let shift = xs.iter().find(|&&x| x > threshold).copied().unwrap_or(0);
            for x in xs.iter_mut() {
                *x -= shift;
            }
        }

        // Calculate visible area
        let visible_rows = (rows as usize).saturating_sub(2);
        let end_row = (state.r0 + visible_rows).min(df.height());

        // Render column headers
        Self::render_headers_xs(df, state, &xs, screen_width, row_num_width, writer)?;

        // Render data rows
        for row_idx in state.r0..end_row {
            let screen_row = (row_idx - state.r0 + 1) as u16;
            execute!(writer, cursor::MoveTo(0, screen_row))?;

            Self::render_row_xs(df, row_idx, state, &xs, screen_width, row_num_width, writer)?;
        }

        // Clear any remaining lines (after header + data rows)
        let first_clear_row = (end_row - state.r0 + 1) as u16; // +1 for header
        for screen_row in first_clear_row..(rows - 1) { // -1 for status bar
            execute!(
                writer,
                cursor::MoveTo(0, screen_row),
                terminal::Clear(terminal::ClearType::CurrentLine)
            )?;
        }

        Ok(())
    }

    /// Render column headers using xs positions (qtv style)
    fn render_headers_xs<W: Write>(df: &DataFrame, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, writer: &mut W) -> Result<()> {
        execute!(
            writer,
            cursor::MoveTo(0, 0),
            terminal::Clear(terminal::ClearType::CurrentLine)
        )?;

        // Render row number header (if showing row numbers)
        if row_num_width > 0 {
            let header = format!("{:>width$} ", "#", width = row_num_width as usize);
            execute!(writer, Print(&header))?;
        }

        for (col_idx, col_name) in df.get_column_names().iter().enumerate() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);

            // Skip if column is completely off-screen left
            if next_x <= 0 {
                continue;
            }
            // Stop if column starts beyond screen
            if x >= screen_width {
                break;
            }

            let is_current = col_idx == state.cc;
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10);

            if is_current {
                execute!(
                    writer,
                    SetBackgroundColor(Color::Blue),
                    SetForegroundColor(Color::White)
                )?;
            }

            let display = format!("{:width$}", col_name, width = col_width as usize);
            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current {
                execute!(writer, ResetColor)?;
            }

            execute!(writer, Print(" "))?;
        }

        Ok(())
    }

    /// Render a single data row using xs positions (qtv style)
    fn render_row_xs<W: Write>(df: &DataFrame, row_idx: usize, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, writer: &mut W) -> Result<()> {
        // Clear the line first
        execute!(writer, terminal::Clear(terminal::ClearType::CurrentLine))?;

        let is_current_row = row_idx == state.cr;

        // Render row number (if showing row numbers)
        if row_num_width > 0 {
            if is_current_row {
                execute!(writer, SetForegroundColor(Color::Yellow))?;
            }
            let row_num = format!("{:>width$} ", row_idx, width = row_num_width as usize);
            execute!(writer, Print(&row_num))?;
            if is_current_row {
                execute!(writer, ResetColor)?;
            }
        }

        for col_idx in 0..df.width() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);

            // Skip if column is completely off-screen left
            if next_x <= 0 {
                continue;
            }
            // Stop if column starts beyond screen
            if x >= screen_width {
                break;
            }

            let is_current_cell = row_idx == state.cr && col_idx == state.cc;

            if is_current_cell {
                execute!(
                    writer,
                    SetBackgroundColor(Color::Yellow),
                    SetForegroundColor(Color::Black)
                )?;
            } else if is_current_row {
                execute!(writer, SetForegroundColor(Color::White))?;
            }

            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10);
            let value = Self::format_value(df, col_idx, row_idx);
            let display = format!("{:width$}", value, width = col_width as usize);

            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current_cell || is_current_row {
                execute!(writer, ResetColor)?;
            }

            execute!(writer, Print(" "))?;
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

    /// Calculate column width by sampling data around current row
    fn calculate_column_width(df: &DataFrame, col_idx: usize, state: &TableState) -> u16 {
        const MAX_WIDTH: usize = 30;
        const MIN_WIDTH: usize = 3;

        let col_name = &df.get_column_names()[col_idx];
        let mut max_width = col_name.len();

        // Sample 2-3 pages around current row for performance
        let page_size = state.viewport.0.saturating_sub(2) as usize;
        let sample_size = (page_size * 3).max(100); // At least 100 rows

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

    /// Format number with commas (e.g., 1000000 -> "1,000,000")
    fn commify(n: usize) -> String {
        let s = n.to_string();
        let mut result = String::new();
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.push(',');
            }
            result.push(c);
        }
        result.chars().rev().collect()
    }

    /// Render status bar at the bottom (qtv style: msg | file | name row/total)
    fn render_status_bar<W: Write>(view: &ViewState, message: &str, rows: u16, cols: u16, writer: &mut W) -> Result<()> {
        let status_row = rows - 1;
        execute!(writer, cursor::MoveTo(0, status_row))?;

        // Format total with commas
        let total = view.row_count();
        let total_str = Self::commify(total);

        let status = if !message.is_empty() {
            format!("{} | {} | {} {}/{}",
                message,
                view.filename.as_deref().unwrap_or(""),
                &view.name,
                view.state.cr,
                total_str
            )
        } else {
            format!("{} | {} {}/{}",
                view.filename.as_deref().unwrap_or("(no file)"),
                &view.name,
                view.state.cr,
                total_str
            )
        };

        // Pad to fill screen width
        let padded = format!("{:width$}", status, width = cols as usize);

        execute!(
            writer,
            SetBackgroundColor(Color::DarkGrey),
            SetForegroundColor(Color::White),
            Print(&padded[..padded.len().min(cols as usize)]),
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
