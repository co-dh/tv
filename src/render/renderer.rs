use crate::app::AppContext;
use crate::state::{TableState, ViewState};
use crate::theme::Theme;
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
        let decimals = app.float_decimals;

        // Get view name for keymap lookup before mutable borrow
        let tab = app.view().map(|v| {
            if v.name.starts_with("Freq:") { "freq" }
            else if v.name == "metadata" { "meta" }
            else if v.name == "correlation" { "corr" }
            else { "table" }
        }).unwrap_or("table");
        let hints = app.keymap.get_hints(tab);

        // Use buffered writer to reduce flickering
        let mut stdout = BufWriter::new(io::stdout());

        let theme = app.theme.clone();
        if let Some(view) = app.view_mut() {
            // Get selection from view (clone to avoid borrow issues)
            let selected_cols = view.selected_cols.clone();
            let selected_rows = view.selected_rows.clone();
            let view_name = view.name.clone();
            Self::render_table(view, rows, cols, &selected_cols, &selected_rows, decimals, &theme, &mut stdout)?;
            if show_info {
                Self::render_info_box(&view_name, stack_len, rows, cols, &hints, &theme, &mut stdout)?;
            }
            Self::render_status_bar(view, &message, rows, cols, &theme, &mut stdout)?;
        } else {
            Self::render_empty_message(&message, rows, cols, &mut stdout)?;
        }

        stdout.flush()?;
        Ok(())
    }

    /// Render the table data
    fn render_table<W: Write>(view: &mut ViewState, rows: u16, cols: u16, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, decimals: usize, theme: &Theme, writer: &mut W) -> Result<()> {
        let df = &view.dataframe;
        let is_correlation = view.name == "correlation";

        // Calculate column widths if needed
        if view.state.need_widths() {
            // Calculate base widths for all columns
            let widths: Vec<u16> = (0..df.width())
                .map(|col_idx| Self::calculate_column_width(df, col_idx, &view.state, decimals))
                .collect();

            view.state.col_widths = widths;
            view.state.widths_row = view.state.cr;
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
            xs.push(*xs.last().unwrap() + col_width + 1);
        }

        // If cursor column right edge exceeds screen width, shift left
        if let Some(cursor_right) = xs.get(state.cc + 1).copied().filter(|&r| r > screen_width) {
            let shift = xs.iter().find(|&&x| x > cursor_right - screen_width).copied().unwrap_or(0);
            for x in xs.iter_mut() {
                *x -= shift;
            }
        }

        // Calculate visible area
        let end_row = (state.r0 + (rows as usize).saturating_sub(2)).min(df.height());

        // Render column headers
        Self::render_headers_xs(df, state, &xs, screen_width, row_num_width, selected_cols, theme, writer)?;

        // Render data rows
        for row_idx in state.r0..end_row {
            let screen_row = (row_idx - state.r0 + 1) as u16;
            execute!(writer, cursor::MoveTo(0, screen_row))?;

            Self::render_row_xs(df, row_idx, state, &xs, screen_width, row_num_width, is_correlation, selected_cols, selected_rows, decimals, theme, writer)?;
        }

        // Clear empty rows between data and status bar
        for screen_row in ((end_row - state.r0 + 1) as u16)..(rows - 1) {
            execute!(
                writer,
                cursor::MoveTo(0, screen_row),
                terminal::Clear(terminal::ClearType::UntilNewLine)
            )?;
        }

        Ok(())
    }

    /// Render column headers using xs positions (qtv style)
    fn render_headers_xs<W: Write>(df: &DataFrame, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, selected_cols: &HashSet<usize>, theme: &Theme, writer: &mut W) -> Result<()> {
        execute!(
            writer,
            cursor::MoveTo(0, 0),
            SetBackgroundColor(theme.header_bg),
            SetForegroundColor(theme.header_fg),
            SetAttribute(Attribute::Bold),
        )?;

        // Render row number header (if showing row numbers)
        if row_num_width > 0 {
            execute!(writer, Print(format!("{:>width$} ", "#", width = row_num_width as usize)))?;
        }

        for (col_idx, col_name) in df.get_column_names().iter().enumerate() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);

            // Skip if column is completely off-screen left
            if next_x <= 0 { continue; }
            // Stop if column starts beyond screen
            if x >= screen_width { break; }

            let is_current = col_idx == state.cc;
            let is_selected = selected_cols.contains(&col_idx);
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10);

            // Current column: cursor background; Selected column: select foreground
            if is_current { execute!(writer, SetBackgroundColor(theme.cursor_bg), SetForegroundColor(theme.cursor_fg))?; }
            else if is_selected { execute!(writer, SetForegroundColor(theme.select_fg))?; }

            let display = format!("{:width$}", col_name, width = col_width as usize);
            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_current || is_selected {
                execute!(writer, ResetColor)?;
                // Re-apply header style after reset
                execute!(writer, SetBackgroundColor(theme.header_bg), SetForegroundColor(theme.header_fg), SetAttribute(Attribute::Bold))?;
            }

            execute!(writer, Print(" "))?;
        }

        // Reset attributes and clear to end of line
        execute!(writer, SetAttribute(Attribute::Reset), terminal::Clear(terminal::ClearType::UntilNewLine))?;

        Ok(())
    }

    /// Render a single data row using xs positions (qtv style)
    fn render_row_xs<W: Write>(df: &DataFrame, row_idx: usize, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, is_correlation: bool, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, decimals: usize, theme: &Theme, writer: &mut W) -> Result<()> {
        let is_cur_row = row_idx == state.cr;
        let is_sel_row = selected_rows.contains(&row_idx);

        // Render row number (if showing row numbers)
        if row_num_width > 0 {
            if is_cur_row { execute!(writer, SetForegroundColor(theme.row_cur_fg))?; }
            else if is_sel_row { execute!(writer, SetForegroundColor(theme.row_num_fg))?; }
            execute!(writer, Print(format!("{:>width$} ", row_idx, width = row_num_width as usize)))?;
            if is_cur_row || is_sel_row { execute!(writer, ResetColor)?; }
        }

        for col_idx in 0..df.width() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }  // off-screen left
            if x >= screen_width { break; }  // beyond screen

            let is_cur_col = col_idx == state.cc;
            let is_cur_cell = is_cur_row && is_cur_col;
            let is_sel = selected_cols.contains(&col_idx);

            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10);
            let value = Self::format_value(df, col_idx, row_idx, decimals);

            // Correlation color (skip first column which is row names)
            let corr_color = if is_correlation && col_idx > 0 { Self::correlation_color(&value) } else { None };

            if is_cur_cell {
                execute!(writer, SetBackgroundColor(theme.cursor_bg), SetForegroundColor(theme.cursor_fg))?;
            } else if is_cur_col {
                execute!(writer, SetBackgroundColor(Color::DarkGrey))?;
                if let Some(fg) = corr_color { execute!(writer, SetForegroundColor(fg))?; }
                else if is_sel { execute!(writer, SetForegroundColor(theme.select_fg))?; }
                else if is_sel_row { execute!(writer, SetForegroundColor(theme.row_num_fg))?; }
            } else if is_sel_row {
                execute!(writer, SetForegroundColor(theme.row_num_fg))?;
            } else if is_sel {
                execute!(writer, SetForegroundColor(theme.select_fg))?;
            } else if let Some(fg) = corr_color {
                execute!(writer, SetForegroundColor(fg))?;
            } else if is_cur_row {
                execute!(writer, SetForegroundColor(theme.row_cur_fg))?;
            }

            let display = format!("{:width$}", value, width = col_width as usize);
            execute!(writer, Print(&display[..display.len().min(col_width as usize)]))?;

            if is_cur_cell || is_cur_col || is_cur_row || corr_color.is_some() || is_sel || is_sel_row {
                execute!(writer, ResetColor)?;
            }
            execute!(writer, Print(" "))?;
        }

        execute!(writer, terminal::Clear(terminal::ClearType::UntilNewLine))?;
        Ok(())
    }

    /// Get color for correlation value using smooth RGB gradient
    fn correlation_color(value: &str) -> Option<Color> {
        let v: f64 = value.parse().ok()?;
        let v = v.clamp(-1.0, 1.0);

        // Diverging colormap: Red (-1) -> Gray (0) -> Green (+1)
        let (r, g, b) = if v < 0.0 {
            // Negative: interpolate from red to gray
            let t = (v + 1.0) as f32; // 0 to 1 as v goes from -1 to 0
            (
                255,
                (180.0 * t) as u8,      // 0 -> 180
                (180.0 * t) as u8,      // 0 -> 180
            )
        } else {
            // Positive: interpolate from gray to green
            let t = v as f32; // 0 to 1 as v goes from 0 to 1
            (
                (180.0 * (1.0 - t)) as u8,  // 180 -> 0
                (180.0 + 75.0 * t) as u8,   // 180 -> 255
                (180.0 * (1.0 - t)) as u8,  // 180 -> 0
            )
        };

        Some(Color::Rgb { r, g, b })
    }

    /// Format a single cell value
    fn format_value(df: &DataFrame, col_idx: usize, row_idx: usize, decimals: usize) -> String {
        let col = df.get_columns()[col_idx].as_materialized_series();
        match col.dtype() {
            DataType::String => col.str().ok().and_then(|s| s.get(row_idx)).unwrap_or("null").to_string(),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                col.get(row_idx).map(|v| match v {
                    AnyValue::Null => "null".to_string(),
                    _ => Self::commify_str(&v.to_string()),
                }).unwrap_or_else(|_| "null".to_string())
            }
            DataType::Float32 | DataType::Float64 => {
                col.get(row_idx).map(|v| match v {
                    AnyValue::Null => "null".to_string(),
                    AnyValue::Float32(f) => Self::commify_float(&format!("{:.prec$}", f, prec = decimals)),
                    AnyValue::Float64(f) => Self::commify_float(&format!("{:.prec$}", f, prec = decimals)),
                    _ => v.to_string(),
                }).unwrap_or_else(|_| "null".to_string())
            }
            DataType::Datetime(_, _) => {
                col.get(row_idx).map(|v| {
                    let s = v.to_string();
                    if s.len() >= 16 { s[..16].to_string() } else { s }
                }).unwrap_or_else(|_| "null".to_string())
            }
            _ => col.get(row_idx).map(|v| v.to_string()).unwrap_or_else(|_| "null".to_string()),
        }
    }

    /// Calculate column width by sampling data around current row
    fn calculate_column_width(df: &DataFrame, col_idx: usize, state: &TableState, decimals: usize) -> u16 {
        const MAX_WIDTH: usize = 30;
        const MIN_WIDTH: usize = 3;

        let mut max_width = df.get_column_names()[col_idx].len();

        // Sample 2-3 pages around current row for performance
        let sample_size = ((state.viewport.0.saturating_sub(2) as usize) * 3).max(100);
        let start_row = state.cr.saturating_sub(sample_size / 2);
        let end_row = (start_row + sample_size).min(df.height());

        for row_idx in start_row..end_row {
            let value = Self::format_value(df, col_idx, row_idx, decimals);
            max_width = max_width.max(value.len());

            if max_width >= MAX_WIDTH { break; }
        }

        max_width.max(MIN_WIDTH).min(MAX_WIDTH) as u16
    }

    /// Format number with commas (e.g., 1000000 -> "1,000,000")
    fn commify(n: usize) -> String { Self::commify_str(&n.to_string()) }

    /// Format integer string with commas
    fn commify_str(s: &str) -> String {
        let (neg, s) = if s.starts_with('-') { (true, &s[1..]) } else { (false, s) };
        let mut result = String::new();
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 { result.push(','); }
            result.push(c);
        }
        if neg { result.push('-'); }
        result.chars().rev().collect()
    }

    /// Format float string with commas in integer part
    fn commify_float(s: &str) -> String {
        if let Some(dot) = s.find('.') {
            format!("{}{}", Self::commify_str(&s[..dot]), &s[dot..])
        } else {
            Self::commify_str(s)
        }
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
            col.str().unwrap().into_iter()
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
    fn render_info_box<W: Write>(_view_name: &str, stack_len: usize, rows: u16, cols: u16, keys: &[(String, &'static str)], theme: &Theme, writer: &mut W) -> Result<()> {
        let max_desc_len = keys.iter().map(|(_, d)| d.len()).max().unwrap_or(10);
        let box_width = max_desc_len + 11;  // key(5) + spaces(4) + borders(2)
        let box_height = keys.len() + 2;    // border top/bottom

        // Position: bottom right, above status bar
        let box_x = cols.saturating_sub(box_width as u16 + 1);
        let box_y = rows.saturating_sub(box_height as u16 + 1);

        execute!(writer, SetForegroundColor(theme.info_border_fg))?;

        // Top border with view info
        let title = format!(" [{}] ", if stack_len > 1 { format!("#{}", stack_len) } else { "tv".to_string() });
        let top_border = format!("┌{}{}┐", title, "─".repeat(box_width.saturating_sub(title.len() + 2)));
        execute!(writer, cursor::MoveTo(box_x, box_y), Print(&top_border))?;

        // Content rows
        for (i, (key, desc)) in keys.iter().enumerate() {
            let row = box_y + 1 + i as u16;
            execute!(writer, cursor::MoveTo(box_x, row))?;
            execute!(writer, SetForegroundColor(theme.info_border_fg), Print("│ "))?;
            execute!(writer, SetForegroundColor(theme.info_key_fg), Print(format!("{:>5}", key)))?;
            execute!(writer, SetForegroundColor(Color::White), Print(format!(" {:width$}", desc, width = box_width - 9)))?;
            execute!(writer, SetForegroundColor(theme.info_border_fg), Print(" │"))?;
        }

        // Bottom border
        execute!(writer, SetForegroundColor(theme.info_border_fg))?;
        let bottom_border = format!("└{}┘", "─".repeat(box_width - 2));
        execute!(writer, cursor::MoveTo(box_x, box_y + box_height as u16 - 1), Print(&bottom_border))?;

        execute!(writer, ResetColor)?;
        Ok(())
    }

    /// Render status bar at the bottom (left: msg/file, middle: col stats, right: row/total)
    fn render_status_bar<W: Write>(view: &ViewState, message: &str, rows: u16, cols: u16, theme: &Theme, writer: &mut W) -> Result<()> {
        execute!(writer, cursor::MoveTo(0, rows - 1))?;

        let total_str = Self::commify(view.rows());

        // Left side: message or filename
        let left = if !message.is_empty() { message.to_string() }
        else if view.name.starts_with("Freq:") || view.name == "metadata" { view.name.clone() }
        else { view.filename.as_deref().unwrap_or("(no file)").to_string() };

        // Column statistics
        let col_stats = if view.cols() > 0 { Self::column_stats(&view.dataframe, view.state.cc) } else { String::new() };

        // Right side: stats + row/total
        let right = if col_stats.is_empty() { format!("{}/{}", view.state.cr, total_str) }
        else { format!("{} {}/{}", col_stats, view.state.cr, total_str) };

        let padding = (cols as usize).saturating_sub(left.len() + right.len()).max(1);
        let status = format!("{}{:width$}{}", left, "", right, width = padding);

        execute!(
            writer,
            SetBackgroundColor(theme.status_bg),
            SetForegroundColor(theme.status_fg),
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

    #[cfg(test)]
    pub fn test_format_value(df: &DataFrame, col_idx: usize, row_idx: usize, decimals: usize) -> String {
        Self::format_value(df, col_idx, row_idx, decimals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_not_commified() {
        // Create DataFrame with null values in integer and float columns
        let df = df! {
            "int_col" => &[Some(1000000i64), None, Some(2000000i64)],
            "float_col" => &[Some(1234.567f64), None, Some(9876.543f64)],
        }.unwrap();

        // Test integer null
        let int_null = Renderer::test_format_value(&df, 0, 1, 3);
        assert_eq!(int_null, "null", "Integer null should be 'null', not 'n,ull'");

        // Test float null
        let float_null = Renderer::test_format_value(&df, 1, 1, 3);
        assert_eq!(float_null, "null", "Float null should be 'null', not 'n,ull'");

        // Verify non-null values are still commified
        let int_val = Renderer::test_format_value(&df, 0, 0, 3);
        assert_eq!(int_val, "1,000,000", "Integer should be commified");

        let float_val = Renderer::test_format_value(&df, 1, 0, 3);
        assert_eq!(float_val, "1,234.567", "Float should be commified");
    }
}
