use crate::app::AppContext;
use crate::state::{TableState, ViewState};
use anyhow::Result;
use crossterm::{
    cursor,
    execute,
    style::{Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor},
    terminal,
};
use polars::prelude::*;
use std::collections::HashSet;
use std::io::{self, BufWriter, Write};

pub struct Renderer;

impl Renderer {
    /// Render the entire screen
    pub fn render(app: &mut AppContext) -> Result<()> {
        let (cols, rows) = terminal::size()?;

        let message = app.message.clone();
        let stack_len = app.stack.len();
        let show_info = app.show_info;

        // Use buffered writer to reduce flickering
        let mut stdout = BufWriter::new(io::stdout());

        if let Some(view) = app.current_view_mut() {
            // Get selection from view (clone to avoid borrow issues)
            let selected_cols = view.selected_cols.clone();
            let selected_rows = view.selected_rows.clone();
            let view_name = view.name.clone();
            Self::render_table(view, rows, cols, &selected_cols, &selected_rows, &mut stdout)?;
            if show_info {
                Self::render_info_box(&view_name, stack_len, rows, cols, &mut stdout)?;
            }
            Self::render_status_bar(view, &message, rows, cols, &mut stdout)?;
        } else {
            Self::render_empty_message(&message, rows, cols, &mut stdout)?;
        }

        stdout.flush()?;
        Ok(())
    }

    /// Render the table data
    fn render_table<W: Write>(view: &mut ViewState, rows: u16, cols: u16, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, writer: &mut W) -> Result<()> {
        let df = &view.dataframe;
        let is_correlation = view.name == "correlation";

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
        Self::render_headers_xs(df, state, &xs, screen_width, row_num_width, selected_cols, writer)?;

        // Render data rows
        for row_idx in state.r0..end_row {
            let screen_row = (row_idx - state.r0 + 1) as u16;
            execute!(writer, cursor::MoveTo(0, screen_row))?;

            Self::render_row_xs(df, row_idx, state, &xs, screen_width, row_num_width, is_correlation, selected_cols, selected_rows, writer)?;
        }

        // Clear empty rows between data and status bar
        let first_empty_row = (end_row - state.r0 + 1) as u16;
        for screen_row in first_empty_row..(rows - 1) {
            execute!(
                writer,
                cursor::MoveTo(0, screen_row),
                terminal::Clear(terminal::ClearType::UntilNewLine)
            )?;
        }

        Ok(())
    }

    /// Render column headers using xs positions (qtv style)
    fn render_headers_xs<W: Write>(df: &DataFrame, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, selected_cols: &HashSet<usize>, writer: &mut W) -> Result<()> {
        execute!(
            writer,
            cursor::MoveTo(0, 0),
            SetAttribute(Attribute::Bold),
            SetAttribute(Attribute::Underlined)
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
            let is_selected = selected_cols.contains(&col_idx);
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10);

            // Current column: dark grey background
            // Selected column: cyan foreground
            if is_current {
                execute!(writer, SetBackgroundColor(Color::DarkGrey))?;
            }
            if is_selected {
                execute!(writer, SetForegroundColor(Color::Cyan))?;
            }

            let display = format!("{:width$}", col_name, width = col_width as usize);
            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current || is_selected {
                execute!(writer, ResetColor)?;
                // Re-apply bold/underline after reset
                execute!(writer, SetAttribute(Attribute::Bold), SetAttribute(Attribute::Underlined))?;
            }

            execute!(writer, Print(" "))?;
        }

        // Reset attributes and clear to end of line
        execute!(
            writer,
            SetAttribute(Attribute::Reset),
            terminal::Clear(terminal::ClearType::UntilNewLine)
        )?;

        Ok(())
    }

    /// Render a single data row using xs positions (qtv style)
    fn render_row_xs<W: Write>(df: &DataFrame, row_idx: usize, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, is_correlation: bool, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, writer: &mut W) -> Result<()> {
        let is_current_row = row_idx == state.cr;
        let is_selected_row = selected_rows.contains(&row_idx);

        // Render row number (if showing row numbers)
        if row_num_width > 0 {
            if is_current_row {
                execute!(writer, SetForegroundColor(Color::Yellow))?;
            } else if is_selected_row {
                execute!(writer, SetForegroundColor(Color::Magenta))?;
            }
            let row_num = format!("{:>width$} ", row_idx, width = row_num_width as usize);
            execute!(writer, Print(&row_num))?;
            if is_current_row || is_selected_row {
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

            let is_current_col = col_idx == state.cc;
            let is_current_cell = is_current_row && is_current_col;
            let is_selected = selected_cols.contains(&col_idx);

            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10);
            let value = Self::format_value(df, col_idx, row_idx);

            // Get correlation color if applicable (skip first column which is row names)
            let corr_color = if is_correlation && col_idx > 0 {
                Self::correlation_color(&value)
            } else {
                None
            };

            if is_current_cell {
                // Current cell: yellow background, black text
                execute!(
                    writer,
                    SetBackgroundColor(Color::Yellow),
                    SetForegroundColor(Color::Black)
                )?;
            } else if is_current_col {
                // Current column: light background
                execute!(writer, SetBackgroundColor(Color::DarkGrey))?;
                if let Some(fg) = corr_color {
                    execute!(writer, SetForegroundColor(fg))?;
                } else if is_selected {
                    execute!(writer, SetForegroundColor(Color::Cyan))?;
                } else if is_selected_row {
                    execute!(writer, SetForegroundColor(Color::Magenta))?;
                }
            } else if is_selected_row {
                // Selected row: magenta text
                execute!(writer, SetForegroundColor(Color::Magenta))?;
            } else if is_selected {
                // Selected column: cyan text
                execute!(writer, SetForegroundColor(Color::Cyan))?;
            } else if let Some(fg) = corr_color {
                // Correlation coloring
                execute!(writer, SetForegroundColor(fg))?;
            } else if is_current_row {
                // Current row: white text
                execute!(writer, SetForegroundColor(Color::White))?;
            }

            let display = format!("{:width$}", value, width = col_width as usize);

            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current_cell || is_current_col || is_current_row || corr_color.is_some() || is_selected || is_selected_row {
                execute!(writer, ResetColor)?;
            }

            execute!(writer, Print(" "))?;
        }

        // Clear to end of line
        execute!(writer, terminal::Clear(terminal::ClearType::UntilNewLine))?;

        Ok(())
    }

    /// Get color for correlation value
    fn correlation_color(value: &str) -> Option<Color> {
        let v: f64 = value.parse().ok()?;

        // Color based on correlation strength
        // Strong positive: bright green
        // Weak positive: dark green
        // Near zero: gray
        // Weak negative: dark red
        // Strong negative: bright red
        Some(if v >= 0.7 {
            Color::Green
        } else if v >= 0.3 {
            Color::DarkGreen
        } else if v > -0.3 {
            Color::DarkGrey
        } else if v > -0.7 {
            Color::DarkRed
        } else {
            Color::Red
        })
    }

    /// Format a single cell value
    fn format_value(df: &DataFrame, col_idx: usize, row_idx: usize) -> String {
        let col = df.get_columns()[col_idx].as_materialized_series();
        if matches!(col.dtype(), DataType::String) {
            col.str()
                .ok()
                .and_then(|s| s.get(row_idx))
                .unwrap_or("null")
                .to_string()
        } else {
            col.get(row_idx).map(|v| v.to_string()).unwrap_or_else(|_| "null".to_string())
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

    /// Calculate column statistics for status bar
    fn column_stats(df: &DataFrame, col_idx: usize) -> String {
        let col = df.get_columns()[col_idx].as_materialized_series();
        let len = col.len();
        if len == 0 {
            return String::new();
        }

        // Count nulls (including empty strings for String type)
        let null_count = if col.dtype() == &DataType::String {
            let str_col = col.str().unwrap();
            str_col.into_iter()
                .filter(|v| v.is_none() || v.map(|s| s.is_empty()).unwrap_or(false))
                .count()
        } else {
            col.null_count()
        };
        let null_pct = 100.0 * null_count as f64 / len as f64;

        match col.dtype() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float32 | DataType::Float64 => {
                // Numerical: min, mean, max, std
                let col_f64 = col.cast(&DataType::Float64).ok();
                if let Some(c) = col_f64 {
                    let min = c.min::<f64>().ok().flatten().unwrap_or(f64::NAN);
                    let max = c.max::<f64>().ok().flatten().unwrap_or(f64::NAN);
                    let mean = c.mean().unwrap_or(f64::NAN);
                    let std = c.std(1).unwrap_or(f64::NAN);
                    if null_pct > 0.0 {
                        format!("null:{:.0}% [{:.2},{:.2},{:.2}] σ{:.2}", null_pct, min, mean, max, std)
                    } else {
                        format!("[{:.2},{:.2},{:.2}] σ{:.2}", min, mean, max, std)
                    }
                } else {
                    String::new()
                }
            }
            _ => {
                // Categorical: distinct count, mode (most frequent value)
                let n_unique = col.n_unique().unwrap_or(0);
                // Get mode by value_counts and taking first
                let mode = col.value_counts(true, false, "cnt".into(), false)
                    .ok()
                    .and_then(|vc| vc.column(col.name().as_str()).ok().cloned())
                    .and_then(|c| c.get(0).ok().map(|v| v.to_string()))
                    .unwrap_or_default();
                let mode_str = if mode.len() > 10 { &mode[..10] } else { &mode };
                if null_pct > 0.0 {
                    format!("null:{:.0}% #{}'{}'", null_pct, n_unique, mode_str)
                } else {
                    format!("#{}'{}'", n_unique, mode_str)
                }
            }
        }
    }

    /// Render info box at bottom right corner (like Kakoune)
    fn render_info_box<W: Write>(view_name: &str, stack_len: usize, rows: u16, cols: u16, writer: &mut W) -> Result<()> {
        // Determine context-sensitive key hints
        let keys: Vec<(&str, &str)> = if view_name.starts_with("Freq:") {
            vec![
                ("Enter", "filter parent"),
                ("Space", "select row"),
                ("g", "top"),
                ("G", "bottom"),
                ("^D", "page down"),
                ("^U", "page up"),
                ("q", "back"),
            ]
        } else if view_name == "metadata" {
            vec![
                ("0", "sel null cols"),
                ("1", "sel single cols"),
                ("D", "delete sel"),
                ("Space", "select row"),
                ("g", "top"),
                ("G", "bottom"),
                ("q", "back"),
            ]
        } else if view_name == "correlation" {
            vec![
                ("g", "top"),
                ("G", "bottom"),
                ("q", "back"),
            ]
        } else {
            vec![
                ("/", "search"),
                ("\\", "filter"),
                ("?", "regex search"),
                ("|", "regex filter"),
                ("n", "next match"),
                ("N", "prev match"),
                ("[", "sort asc"),
                ("]", "sort desc"),
                ("F", "freq table"),
                ("M", "metadata"),
                ("C", "correlation"),
                ("Space", "select col"),
                ("D", "delete col"),
                ("g", "top"),
                ("G", "bottom"),
                ("^D", "page down"),
                ("^U", "page up"),
                ("L", "load file"),
                ("S", "save file"),
                ("q", "quit"),
            ]
        };

        // Calculate box dimensions
        let max_desc_len = keys.iter().map(|(_, d)| d.len()).max().unwrap_or(10);
        let box_width = max_desc_len + 11; // key(5) + spaces(4) + borders(2)
        let box_height = keys.len() + 2; // border top/bottom

        // Position: bottom right, above status bar
        let box_x = cols.saturating_sub(box_width as u16 + 1);
        let box_y = rows.saturating_sub(box_height as u16 + 1);

        // Draw box with brighter colors
        execute!(writer, SetForegroundColor(Color::Cyan))?;

        // Top border with view info
        let title = format!(" [{}] ", if stack_len > 1 { format!("#{}", stack_len) } else { "tv".to_string() });
        let top_border = format!("┌{}{}┐", title, "─".repeat(box_width.saturating_sub(title.len() + 2)));
        execute!(writer, cursor::MoveTo(box_x, box_y), Print(&top_border))?;

        // Content rows
        for (i, (key, desc)) in keys.iter().enumerate() {
            let row = box_y + 1 + i as u16;
            execute!(writer, cursor::MoveTo(box_x, row))?;
            execute!(writer, SetForegroundColor(Color::Cyan), Print("│ "))?;
            execute!(writer, SetForegroundColor(Color::Yellow), Print(format!("{:>5}", key)))?;
            execute!(writer, SetForegroundColor(Color::White), Print(format!(" {:width$}", desc, width = box_width - 9)))?;
            execute!(writer, SetForegroundColor(Color::Cyan), Print(" │"))?;
        }

        // Bottom border
        execute!(writer, SetForegroundColor(Color::Cyan))?;
        let bottom_border = format!("└{}┘", "─".repeat(box_width - 2));
        execute!(writer, cursor::MoveTo(box_x, box_y + box_height as u16 - 1), Print(&bottom_border))?;

        execute!(writer, ResetColor)?;
        Ok(())
    }

    /// Render status bar at the bottom (left: msg/file, middle: col stats, right: row/total)
    fn render_status_bar<W: Write>(view: &ViewState, message: &str, rows: u16, cols: u16, writer: &mut W) -> Result<()> {
        let status_row = rows - 1;
        execute!(writer, cursor::MoveTo(0, status_row))?;

        // Format total with commas
        let total = view.row_count();
        let total_str = Self::commify(total);

        // Left side: message or filename (for special views like Freq:*, show the name)
        let left = if !message.is_empty() {
            message.to_string()
        } else if view.name.starts_with("Freq:") || view.name == "metadata" {
            view.name.clone()
        } else {
            view.filename.as_deref().unwrap_or("(no file)").to_string()
        };

        // Column statistics (for right side)
        let col_stats = if view.col_count() > 0 {
            Self::column_stats(&view.dataframe, view.state.cc)
        } else {
            String::new()
        };

        // Right side: stats + row/total
        let right = if col_stats.is_empty() {
            format!("{}/{}", view.state.cr, total_str)
        } else {
            format!("{} {}/{}", col_stats, view.state.cr, total_str)
        };

        // Calculate padding between left and right
        let total_len = left.len() + right.len();
        let padding = if (cols as usize) > total_len {
            cols as usize - total_len
        } else {
            1
        };

        let status = format!("{}{:width$}{}", left, "", right, width = padding);

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
