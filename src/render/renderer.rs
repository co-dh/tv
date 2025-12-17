use crate::app::AppContext;
use crate::backend::{Backend, Polars, commify};
use crate::state::{TableState, ViewState};
use crate::theme::Theme;
use polars::prelude::*;
use ratatui::prelude::*;
use ratatui::style::{Color as RColor, Modifier, Style};
use ratatui::widgets::Tabs;
use std::collections::HashSet;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

/// Debug log to /tmp/tv.debug.log with timestamp
fn dbg_log(msg: &str) {
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/tv.debug.log") {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        let _ = writeln!(f, "{} {}", ts, msg);
    }
}

pub struct Renderer;

impl Renderer {
    /// Render entire screen to ratatui frame (diff-based)
    pub fn render(frame: &mut Frame, app: &mut AppContext) {
        let area = frame.area();
        let message = app.message.clone();
        let stack_len = app.stack.len();
        let stack_names = app.stack.names();
        let show_info = app.show_info;
        let decimals = app.float_decimals;
        let is_loading = app.is_loading();

        // Get view name for keymap lookup
        let tab = app.view().map(|v| app.plugins.tab(&v.name)).unwrap_or("table");
        let hints = app.keymap.get_hints(tab);
        let theme = app.theme.clone();

        if let Some(view) = app.view_mut() {
            let selected_cols = view.selected_cols.clone();
            let selected_rows = view.selected_rows.clone();
            let view_name = view.name.clone();
            let show_tabs = stack_names.len() > 1;
            Self::render_table(frame, view, area, &selected_cols, &selected_rows, decimals, &theme, show_tabs);
            if show_info {
                Self::render_info_box(frame, &view_name, stack_len, area, &hints, &theme);
            }
            if show_tabs {
                Self::render_tabs(frame, &stack_names, area, &theme);
            }
            Self::render_status_bar(frame, view, &message, is_loading, area, &theme);
        } else {
            Self::empty_msg(frame, &message, area);
            if show_info {
                Self::render_info_box(frame, "table", stack_len, area, &hints, &theme);
            }
        }
    }

    /// Render table data
    fn render_table(frame: &mut Frame, view: &mut ViewState, area: Rect, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, decimals: usize, theme: &Theme, show_tabs: bool) {
        const CACHE_SIZE: usize = 100_000;  // cache 100k rows
        // For lazy parquet views, fetch visible rows from disk (with cache)
        let lazy_offset = if let Some(ref path) = view.parquet_path {
            let rows_needed = area.height as usize + 10;
            let (r0, rend) = (view.state.r0, view.state.r0 + rows_needed);
            // Check if visible range is within cache
            let in_cache = view.fetch_cache.map(|(s, e)| r0 >= s && rend <= e).unwrap_or(false);
            if !in_cache {
                // Fetch 100k rows centered around current position
                let start = r0.saturating_sub(CACHE_SIZE / 4);  // 25k before
                dbg_log(&format!("fetch parquet start={} rows={} filter={:?}", start, CACHE_SIZE, view.filter_clause));
                let w = view.filter_clause.as_deref().unwrap_or("TRUE");
                let df = Polars.fetch_sel(path, &view.col_names, w, start, CACHE_SIZE);
                if let Ok(df) = df {
                    let fetched = df.height();
                    view.dataframe = df;
                    view.fetch_cache = Some((start, start + fetched));
                }
            }
            // Return offset: df row 0 = cache start
            view.fetch_cache.map(|(s, _)| s).unwrap_or(0)
        } else { 0 };
        let df = &view.dataframe;
        let total_rows = view.rows();  // use disk_rows for parquet
        let is_correlation = view.name == "correlation";

        // Calculate column widths if needed
        if view.state.need_widths() {
            let mut widths: Vec<u16> = (0..df.width())
                .map(|col_idx| Self::col_width(df, col_idx, &view.state, decimals))
                .collect();
            // Last column gets remaining screen width
            if !widths.is_empty() {
                let used: u16 = widths.iter().take(widths.len() - 1).map(|w| w + 1).sum();
                let avail = area.width.saturating_sub(used + 1);
                let last = widths.last_mut().unwrap();
                *last = (*last).max(avail.min(200));  // cap at 200 to avoid huge widths
            }
            view.state.col_widths = widths;
            view.state.widths_row = view.state.cr;
        }

        let state = &view.state;

        if df.height() == 0 || df.width() == 0 {
            let buf = frame.buffer_mut();
            buf.set_string(0, 0, "(empty table)", Style::default());
            return;
        }

        // Row number width (use total_rows for lazy parquet)
        let row_num_width = if view.show_row_numbers {
            total_rows.to_string().len().max(3) as u16
        } else { 0 };
        let screen_width = area.width.saturating_sub(if row_num_width > 0 { row_num_width + 1 } else { 0 }) as i32;

        // Calculate xs - x position for each column (qtv style)
        let mut xs: Vec<i32> = Vec::with_capacity(df.width() + 1);
        xs.push(0);
        for col_idx in 0..df.width() {
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as i32;
            xs.push(*xs.last().unwrap() + col_width + 1);
        }

        // Shift if cursor column exceeds screen
        if let Some(cursor_right) = xs.get(state.cc + 1).copied().filter(|&r| r > screen_width) {
            let shift = xs.iter().find(|&&x| x > cursor_right - screen_width).copied().unwrap_or(0);
            for x in xs.iter_mut() { *x -= shift; }
        }

        // Reserve rows: header(1) + footer_header(1) + status(1) + tabs(1 if shown)
        let bottom_reserve = if show_tabs { 4 } else { 3 };
        let end_row = (state.r0 + (area.height as usize).saturating_sub(bottom_reserve)).min(total_rows);

        let col_sep = view.col_separator;

        // Render headers
        Self::render_headers_xs(frame, df, state, &xs, screen_width, row_num_width, selected_cols, col_sep, theme, area);

        // Render data rows (for lazy parquet, df_idx = row_idx - lazy_offset)
        for row_idx in state.r0..end_row {
            let df_idx = row_idx - lazy_offset;
            if df_idx >= df.height() { break; }  // fetched window exhausted
            let screen_row = (row_idx - state.r0 + 1) as u16;
            Self::render_row_xs(frame, df, df_idx, row_idx, state, &xs, screen_width, row_num_width, is_correlation, selected_cols, selected_rows, col_sep, decimals, theme, area, screen_row);
        }

        // Draw separator bar if set (stop before tabs/status)
        if let Some(sep_col) = col_sep {
            if sep_col < df.width() {
                let sep_x = xs.get(sep_col).copied().unwrap_or(0);
                if sep_x > 0 && sep_x < screen_width {
                    let px = (sep_x - 1) as u16 + row_num_width + if row_num_width > 0 { 1 } else { 0 };
                    let buf = frame.buffer_mut();
                    let sep_style = Style::default().fg(to_rcolor(theme.info_border_fg));
                    let sep_end = area.height.saturating_sub(bottom_reserve as u16);
                    for y in 0..sep_end {
                        if px < area.width { buf[(px, y)].set_char('│').set_style(sep_style); }
                    }
                }
            }
        }

        // Clear empty rows (stop before tabs/status)
        let clear_end = area.height.saturating_sub(bottom_reserve as u16);
        let buf = frame.buffer_mut();
        for screen_row in ((end_row - state.r0 + 1) as u16)..clear_end {
            for x in 0..area.width {
                buf[(x, screen_row)].reset();
            }
        }

        // Footer header (aligned with table)
        Self::render_header_footer(frame, df, state, &xs, screen_width, row_num_width, theme, area, show_tabs);
    }

    /// Render column headers
    fn render_headers_xs(frame: &mut Frame, df: &DataFrame, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, selected_cols: &HashSet<usize>, _col_sep: Option<usize>, theme: &Theme, area: Rect) {
        let buf = frame.buffer_mut();
        let header_style = Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.header_fg)).add_modifier(Modifier::BOLD);

        // Fill header row with header style first
        for x in 0..area.width { buf[(x, 0)].set_style(header_style); buf[(x, 0)].set_char(' '); }

        let mut x_pos = 0u16;

        // Row number header
        if row_num_width > 0 {
            let s = format!("{:>width$} ", "#", width = row_num_width as usize);
            for (i, ch) in s.chars().enumerate() {
                if x_pos + i as u16 >= area.width { break; }
                buf[(x_pos + i as u16, 0)].set_char(ch);
            }
            x_pos += row_num_width + 1;
        }

        for (col_idx, col_name) in df.get_column_names().iter().enumerate() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_current = col_idx == state.cc;
            let is_selected = selected_cols.contains(&col_idx);
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;

            let style = if is_current {
                Style::default().bg(to_rcolor(theme.cursor_bg)).fg(to_rcolor(theme.cursor_fg)).add_modifier(Modifier::BOLD)
            } else if is_selected {
                Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.select_fg)).add_modifier(Modifier::BOLD)
            } else { header_style };

            let start_x = x.max(0) as u16 + x_pos;
            let display = format!("{:width$}", col_name, width = col_width);

            for (i, ch) in display.chars().take(col_width).enumerate() {
                let px = start_x + i as u16;
                if px >= area.width { break; }
                buf[(px, 0)].set_char(ch).set_style(style);
            }

            // Separator space
            let sep_x = start_x + col_width as u16;
            if sep_x < area.width {
                buf[(sep_x, 0)].set_char(' ').set_style(header_style);
            }
        }
    }

    /// Render a single data row
    // df_idx: index into dataframe, row_idx: actual row number in file (for display/cursor)
    fn render_row_xs(frame: &mut Frame, df: &DataFrame, df_idx: usize, row_idx: usize, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, is_correlation: bool, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, _col_sep: Option<usize>, decimals: usize, theme: &Theme, area: Rect, screen_row: u16) {
        let buf = frame.buffer_mut();
        let is_cur_row = row_idx == state.cr;
        let is_sel_row = selected_rows.contains(&row_idx);

        // Clear row first
        for x in 0..area.width { buf[(x, screen_row)].reset(); }

        let mut x_pos = 0u16;

        // Row number
        if row_num_width > 0 {
            let style = if is_cur_row { Style::default().fg(to_rcolor(theme.row_cur_fg)) }
                       else if is_sel_row { Style::default().fg(to_rcolor(theme.row_num_fg)) }
                       else { Style::default() };
            let s = format!("{:>width$} ", row_idx, width = row_num_width as usize);
            for (i, ch) in s.chars().enumerate() {
                if x_pos + i as u16 >= area.width { break; }
                buf[(x_pos + i as u16, screen_row)].set_char(ch).set_style(style);
            }
            x_pos += row_num_width + 1;
        }

        for col_idx in 0..df.width() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_cur_col = col_idx == state.cc;
            let is_cur_cell = is_cur_row && is_cur_col;
            let is_sel = selected_cols.contains(&col_idx);

            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;
            let value = Self::format_value(df, col_idx, df_idx, decimals);

            // Correlation color
            let corr_color = if is_correlation && col_idx > 0 { Self::correlation_color(&value) } else { None };

            let style = if is_cur_cell {
                Style::default().bg(to_rcolor(theme.cursor_bg)).fg(to_rcolor(theme.cursor_fg))
            } else if is_cur_col {
                let fg = corr_color.map(to_rcolor)
                    .or_else(|| if is_sel { Some(to_rcolor(theme.select_fg)) } else { None })
                    .or_else(|| if is_sel_row { Some(to_rcolor(theme.row_num_fg)) } else { None })
                    .unwrap_or(RColor::Reset);
                Style::default().bg(RColor::DarkGray).fg(fg)
            } else if is_sel_row {
                Style::default().fg(to_rcolor(theme.row_num_fg))
            } else if is_sel {
                Style::default().fg(to_rcolor(theme.select_fg))
            } else if let Some(c) = corr_color {
                Style::default().fg(to_rcolor(c))
            } else if is_cur_row {
                Style::default().fg(to_rcolor(theme.row_cur_fg))
            } else { Style::default() };

            let start_x = x.max(0) as u16 + x_pos;
            let display = format!("{:width$}", value, width = col_width);

            for (i, ch) in display.chars().take(col_width).enumerate() {
                let px = start_x + i as u16;
                if px >= area.width { break; }
                buf[(px, screen_row)].set_char(ch).set_style(style);
            }

            // Separator
            let sep_x = start_x + col_width as u16;
            if sep_x < area.width { buf[(sep_x, screen_row)].set_char(' '); }
        }
    }

    /// Get color for correlation value
    fn correlation_color(value: &str) -> Option<ratatui::crossterm::style::Color> {
        use ratatui::crossterm::style::Color;
        let v: f64 = value.parse().ok()?;
        let v = v.clamp(-1.0, 1.0);

        let (r, g, b) = if v < 0.0 {
            let t = (v + 1.0) as f32;
            (255, (180.0 * t) as u8, (180.0 * t) as u8)
        } else {
            let t = v as f32;
            ((180.0 * (1.0 - t)) as u8, (180.0 + 75.0 * t) as u8, (180.0 * (1.0 - t)) as u8)
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
            DataType::Time => {
                // Format Time as HH:MM:SS.mmm
                col.get(row_idx).map(|v| match v {
                    AnyValue::Time(ns) => {
                        let secs = ns / 1_000_000_000;
                        let ms = (ns % 1_000_000_000) / 1_000_000;
                        format!("{:02}:{:02}:{:02}.{:03}", secs / 3600, (secs % 3600) / 60, secs % 60, ms)
                    }
                    _ => v.to_string(),
                }).unwrap_or_else(|_| "null".to_string())
            }
            _ => col.get(row_idx).map(|v| v.to_string()).unwrap_or_else(|_| "null".to_string()),
        }
    }

    /// Calculate column width
    fn col_width(df: &DataFrame, col_idx: usize, state: &TableState, decimals: usize) -> u16 {
        const MAX_WIDTH: usize = 30;
        const MIN_WIDTH: usize = 3;

        let mut max_width = df.get_column_names()[col_idx].len();
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

    /// Format number with commas (handles negatives)
    fn commify_str(s: &str) -> String {
        if s.starts_with('-') { format!("-{}", commify(&s[1..])) } else { commify(s) }
    }

    /// Format float with commas in integer part
    fn commify_float(s: &str) -> String {
        if let Some(dot) = s.find('.') {
            format!("{}{}", Self::commify_str(&s[..dot]), &s[dot..])
        } else { Self::commify_str(s) }
    }

    /// Calculate column statistics
    fn column_stats(df: &DataFrame, col_idx: usize) -> String {
        let col = df.get_columns()[col_idx].as_materialized_series();
        let len = col.len();
        if len == 0 { return String::new(); }

        let null_count = if col.dtype() == &DataType::String {
            col.str().unwrap().into_iter()
                .filter(|v| v.is_none() || v.map(|s| s.is_empty()).unwrap_or(false))
                .count()
        } else { col.null_count() };
        let null_pct = 100.0 * null_count as f64 / len as f64;

        match col.dtype() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float32 | DataType::Float64 => {
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
                } else { String::new() }
            }
            _ => {
                let n_unique = col.n_unique().unwrap_or(0);
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

    /// Render info box using ratatui widgets
    fn render_info_box(frame: &mut Frame, _view_name: &str, stack_len: usize, area: Rect, keys: &[(String, &'static str)], theme: &Theme) {
        use ratatui::widgets::{Block, Borders, Paragraph, Clear};
        use ratatui::text::{Line, Span};

        let max_desc_len = keys.iter().map(|(_, d)| d.len()).max().unwrap_or(10);
        let box_width = (max_desc_len + 11) as u16;
        let box_height = (keys.len() + 2) as u16;

        let box_x = area.width.saturating_sub(box_width + 1);
        let box_y = area.height.saturating_sub(box_height + 1);
        let box_area = Rect::new(box_x, box_y, box_width, box_height);

        // Clear area first
        frame.render_widget(Clear, box_area);

        // Block with border and title
        let title = if stack_len > 1 { format!(" [#{}] ", stack_len) } else { " [tv] ".to_string() };
        let border_style = Style::default().fg(to_rcolor(theme.info_border_fg));
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(title);

        // Build styled lines for content
        let key_style = Style::default().fg(to_rcolor(theme.info_key_fg));
        let text_style = Style::default().fg(RColor::White);
        let lines: Vec<Line> = keys.iter().map(|(key, desc)| {
            Line::from(vec![
                Span::styled(format!("{:>5}", key), key_style),
                Span::raw(" "),
                Span::styled(*desc, text_style),
            ])
        }).collect();

        let para = Paragraph::new(lines).block(block);
        frame.render_widget(para, box_area);
    }

    /// Render view stack as tabs
    fn render_tabs(frame: &mut Frame, names: &[String], area: Rect, theme: &Theme) {
        let row = area.height - 2;
        let tab_area = Rect::new(0, row, area.width, 1);
        // Shorten names: extract filename, truncate to 20 chars
        let short: Vec<String> = names.iter().map(|s| {
            let n = s.rsplit('/').next().unwrap_or(s);  // get filename
            let n = n.split(':').next().unwrap_or(n);   // remove :suffix
            if n.len() > 20 { format!("{}…", &n[..19]) } else { n.to_string() }
        }).collect();
        // Fill background
        let buf = frame.buffer_mut();
        let bg = to_rcolor(theme.tab_bg);
        for x in 0..area.width { buf[(x, row)].set_style(Style::default().bg(bg)).set_char(' '); }
        // Render tabs
        let selected = names.len().saturating_sub(1);
        let tabs = Tabs::new(short.iter().map(|s| s.as_str()))
            .select(selected)
            .style(Style::default().fg(to_rcolor(theme.status_fg)).bg(bg))
            .highlight_style(Style::default().fg(to_rcolor(theme.header_fg)).bg(bg).add_modifier(Modifier::BOLD))
            .divider("│");
        frame.render_widget(tabs, tab_area);
    }

    /// Render column header above tabs/status (footer header) - aligned with table
    fn render_header_footer(frame: &mut Frame, df: &DataFrame, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, theme: &Theme, area: Rect, show_tabs: bool) {
        let row = area.height.saturating_sub(if show_tabs { 3 } else { 2 });
        if row == 0 { return; }
        let buf = frame.buffer_mut();
        let style = Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.header_fg));
        // Fill row
        for x in 0..area.width { buf[(x, row)].set_style(style); buf[(x, row)].set_char(' '); }
        let x_pos = if row_num_width > 0 { row_num_width + 1 } else { 0 };
        for (col_idx, col_name) in df.get_column_names().iter().enumerate() {
            let x = xs.get(col_idx).copied().unwrap_or(0);
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;
            let start_x = x.max(0) as u16 + x_pos;
            let display = format!("{:width$}", col_name.as_str(), width = col_width);
            for (i, ch) in display.chars().take(col_width).enumerate() {
                let px = start_x + i as u16;
                if px >= area.width { break; }
                buf[(px, row)].set_char(ch);
            }
        }
    }

    /// Render status bar
    fn render_status_bar(frame: &mut Frame, view: &mut ViewState, message: &str, is_loading: bool, area: Rect, theme: &Theme) {
        let row = area.height - 1;
        let buf = frame.buffer_mut();
        let style = Style::default().bg(to_rcolor(theme.status_bg)).fg(to_rcolor(theme.status_fg));

        // Fill status bar
        for x in 0..area.width { buf[(x, row)].set_style(style); buf[(x, row)].set_char(' '); }

        // Show total rows: just disk_rows if set, else dataframe height
        let total_str = commify(&view.rows().to_string());

        let left = if !message.is_empty() { message.to_string() }
        else if view.name.starts_with("Freq:") || view.name == "metadata" {
            // Show parent name and row count for Meta/Freq views
            let pn = view.parent_name.as_deref().unwrap_or("");
            let pr = view.parent_rows.map(|n| format!(" ({})", commify(&n.to_string()))).unwrap_or_default();
            format!("{} <- {}{}", view.name, pn, pr)
        }
        else { view.filename.as_deref().unwrap_or("(no file)").to_string() };

        // Use cached stats if column unchanged
        let col_stats = if view.cols() > 0 {
            let cc = view.state.cc;
            if let Some((cached_cc, ref s)) = view.stats_cache {
                if cached_cc == cc { s.clone() }
                else {
                    let s = Self::column_stats(&view.dataframe, cc);
                    view.stats_cache = Some((cc, s.clone()));
                    s
                }
            } else {
                let s = Self::column_stats(&view.dataframe, cc);
                view.stats_cache = Some((cc, s.clone()));
                s
            }
        } else { String::new() };

        let partial = if is_loading || view.partial { " Partial" } else { "" };
        let right = if col_stats.is_empty() { format!("{}/{}{}", view.state.cr, total_str, partial) }
        else { format!("{} {}/{}{}", col_stats, view.state.cr, total_str, partial) };

        let padding = (area.width as usize).saturating_sub(left.len() + right.len()).max(1);
        let status = format!("{}{:width$}{}", left, "", right, width = padding);

        for (i, ch) in status.chars().enumerate() {
            if i >= area.width as usize { break; }
            buf[(i as u16, row)].set_char(ch);
        }
    }

    /// Render empty message
    fn empty_msg(frame: &mut Frame, message: &str, area: Rect) {
        let buf = frame.buffer_mut();
        let y = area.height / 2;
        for (i, ch) in message.chars().enumerate() {
            if i >= area.width as usize { break; }
            buf[(i as u16, y)].set_char(ch);
        }
    }

    #[cfg(test)]
    pub fn test_format_value(df: &DataFrame, col_idx: usize, row_idx: usize, decimals: usize) -> String {
        Self::format_value(df, col_idx, row_idx, decimals)
    }
}

/// Convert crossterm Color to ratatui Color
fn to_rcolor(c: ratatui::crossterm::style::Color) -> RColor {
    use ratatui::crossterm::style::Color;
    match c {
        Color::Reset => RColor::Reset,
        Color::Black => RColor::Black,
        Color::DarkGrey => RColor::DarkGray,
        Color::Red => RColor::Red,
        Color::DarkRed => RColor::Red,
        Color::Green => RColor::Green,
        Color::DarkGreen => RColor::Green,
        Color::Yellow => RColor::Yellow,
        Color::DarkYellow => RColor::Yellow,
        Color::Blue => RColor::Blue,
        Color::DarkBlue => RColor::Blue,
        Color::Magenta => RColor::Magenta,
        Color::DarkMagenta => RColor::Magenta,
        Color::Cyan => RColor::Cyan,
        Color::DarkCyan => RColor::Cyan,
        Color::White => RColor::White,
        Color::Grey => RColor::Gray,
        Color::Rgb { r, g, b } => RColor::Rgb(r, g, b),
        Color::AnsiValue(v) => RColor::Indexed(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_not_commified() {
        let df = df! {
            "int_col" => &[Some(1000000i64), None, Some(2000000i64)],
            "float_col" => &[Some(1234.567f64), None, Some(9876.543f64)],
        }.unwrap();

        let int_null = Renderer::test_format_value(&df, 0, 1, 3);
        assert_eq!(int_null, "null", "Integer null should be 'null', not 'n,ull'");

        let float_null = Renderer::test_format_value(&df, 1, 1, 3);
        assert_eq!(float_null, "null", "Float null should be 'null', not 'n,ull'");

        let int_val = Renderer::test_format_value(&df, 0, 0, 3);
        assert_eq!(int_val, "1,000,000", "Integer should be commified");

        let float_val = Renderer::test_format_value(&df, 1, 0, 3);
        assert_eq!(float_val, "1,234.567", "Float should be commified");
    }
}
