use crate::app::AppContext;
use crate::dynload;
use crate::table::{Cell, Table};
use crate::utils::commify;
use crate::state::{TableState, ViewKind, ViewSource, ViewState};
use crate::theme::Theme;
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
        let info_mode = app.info_mode;
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
            let prql = view.prql.clone();
            let show_tabs = stack_names.len() > 1;
            Self::render_table(frame, view, area, &selected_cols, &selected_rows, decimals, &theme, show_tabs);
            if info_mode > 0 {
                Self::render_info_box(frame, &view_name, stack_len, area, &hints, &theme, info_mode, &prql);
            }
            if show_tabs {
                Self::render_tabs(frame, &stack_names, area, &theme);
            }
            Self::render_status_bar(frame, view, &message, is_loading, area, &theme);
        } else {
            Self::empty_msg(frame, &message, area);
            if info_mode > 0 {
                Self::render_info_box(frame, "table", stack_len, area, &hints, &theme, info_mode, "");
            }
        }
    }

    /// Render table data
    fn render_table(frame: &mut Frame, view: &mut ViewState, area: Rect, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, decimals: usize, theme: &Theme, show_tabs: bool) {
        // Parquet cache: keeps 10k row window in memory to avoid SQL queries on every render frame.
        // Without cache: 60fps = 60 SQL queries/sec. Cache reduces to "only when scrolling outside window".
        // Not used for in-memory CSV.
        const CACHE_SIZE: usize = 10_000;
        let lazy_offset = if let ViewSource::Parquet { ref path, .. } = view.source {
            let rows_needed = area.height as usize + 10;
            let (r0, rend) = (view.state.r0, view.state.r0 + rows_needed);
            // Check if visible range is within cache
            let in_cache = view.cache.fetch.map(|(s, e)| r0 >= s && rend <= e).unwrap_or(false);
            if !in_cache {
                // Fetch 10k rows centered around current position via plugin
                let start = r0.saturating_sub(CACHE_SIZE / 4);
                dbg_log(&format!("fetch parquet start={} rows={} filter={:?}", start, CACHE_SIZE, view.filter));
                if let Some(plugin) = dynload::get() {
                    let w = view.filter.as_deref().unwrap_or("TRUE");
                    if let Some(t) = plugin.fetch_where(path, w, start, CACHE_SIZE) {
                        let fetched = t.rows();  // Table trait method
                        view.data = dynload::to_box_table(&t);
                        view.cache.fetch = Some((start, start + fetched));
                    }
                }
            }
            // Return offset: df row 0 = cache start
            view.cache.fetch.map(|(s, _)| s).unwrap_or(0)
        } else { 0 };

        // Use Table trait for polars-free rendering
        let table = view.data.as_ref();
        let total_rows = view.rows();  // use disk_rows for parquet
        let is_correlation = view.kind == ViewKind::Corr;

        // Calculate column widths if needed (based on content, don't extend last col)
        if view.state.need_widths() {
            let widths: Vec<u16> = (0..table.cols())
                .map(|col_idx| Self::col_width(table, col_idx, &view.state, decimals))
                .collect();
            view.state.col_widths = widths;
            view.state.widths_row = view.state.cr;
        }

        let state = &view.state;

        if table.rows() == 0 || table.cols() == 0 {
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
        let mut xs: Vec<i32> = Vec::with_capacity(table.cols() + 1);
        xs.push(0);
        for col_idx in 0..table.cols() {
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
        Self::render_headers_xs(frame, table, state, &xs, screen_width, row_num_width, selected_cols, col_sep, theme, area);

        // Render data rows (for lazy parquet, df_idx = row_idx - lazy_offset)
        for row_idx in state.r0..end_row {
            let df_idx = row_idx - lazy_offset;
            if df_idx >= table.rows() { break; }  // fetched window exhausted
            let screen_row = (row_idx - state.r0 + 1) as u16;
            Self::render_row_xs(frame, table, df_idx, row_idx, state, &xs, screen_width, row_num_width, is_correlation, selected_cols, selected_rows, col_sep, decimals, theme, area, screen_row);
        }

        // Draw separator bar if set (stop before tabs/status)
        if let Some(sep_col) = col_sep {
            if sep_col < table.cols() {
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
        Self::render_header_footer(frame, table, state, &xs, screen_width, row_num_width, theme, area, show_tabs);
    }

    /// Render column headers
    fn render_headers_xs(frame: &mut Frame, table: &dyn Table, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, selected_cols: &HashSet<usize>, _col_sep: Option<usize>, theme: &Theme, area: Rect) {
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

        for col_idx in 0..table.cols() {
            let col_name = table.col_name(col_idx).unwrap_or_default();
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
                // Selected column: cyan foreground
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
    fn render_row_xs(frame: &mut Frame, table: &dyn Table, df_idx: usize, row_idx: usize, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, is_correlation: bool, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, _col_sep: Option<usize>, decimals: usize, theme: &Theme, area: Rect, screen_row: u16) {
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

        for col_idx in 0..table.cols() {
            let x = xs[col_idx];
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_cur_col = col_idx == state.cc;
            let is_cur_cell = is_cur_row && is_cur_col;
            let is_sel = selected_cols.contains(&col_idx);

            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;
            let value = Self::format_cell(table, col_idx, df_idx, decimals);

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
                // Selected column: cyan foreground
                Style::default().fg(to_rcolor(theme.select_fg))
            } else if let Some(c) = corr_color {
                Style::default().fg(to_rcolor(c))
            } else if is_cur_row {
                Style::default().fg(to_rcolor(theme.row_cur_fg))
            } else { Style::default() };

            let start_x = x.max(0) as u16 + x_pos;
            // Right-align numeric columns
            let is_num = table.col_type(col_idx).is_numeric();
            let display = if is_num { format!("{:>width$}", value, width = col_width) }
                         else { format!("{:width$}", value, width = col_width) };

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

    /// Format a single cell value using Table trait (polars-free)
    fn format_cell(table: &dyn Table, col_idx: usize, row_idx: usize, decimals: usize) -> String {
        let cell = table.cell(row_idx, col_idx);
        match cell {
            Cell::Null => "null".to_string(),
            Cell::Int(n) => Self::commify_str(&n.to_string()),
            Cell::Float(f) => Self::commify_float(&format!("{:.prec$}", f, prec = decimals)),
            Cell::DateTime(s) => if s.len() >= 16 { s[..16].to_string() } else { s },
            _ => cell.format(decimals),
        }
    }

    /// Calculate column width using Table trait (polars-free)
    fn col_width(table: &dyn Table, col_idx: usize, state: &TableState, decimals: usize) -> u16 {
        const MIN_WIDTH: usize = 3;
        // Path columns get more width (for lr view)
        let col_name = table.col_name(col_idx).unwrap_or_default();
        let max_width_limit = if col_name == "path" { 80 } else { 30 };

        let mut max_width = col_name.len();
        let sample_size = ((state.viewport.0.saturating_sub(2) as usize) * 3).max(100);
        let start_row = state.cr.saturating_sub(sample_size / 2);
        let end_row = (start_row + sample_size).min(table.rows());

        for row_idx in start_row..end_row {
            let value = Self::format_cell(table, col_idx, row_idx, decimals);
            max_width = max_width.max(value.len());
            if max_width >= max_width_limit { break; }
        }

        max_width.max(MIN_WIDTH).min(max_width_limit) as u16
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

    /// Render info box using ratatui widgets
    fn render_info_box(frame: &mut Frame, _view_name: &str, stack_len: usize, area: Rect, keys: &[(String, &'static str)], theme: &Theme, info_mode: u8, prql: &str) {
        use ratatui::widgets::{Block, Borders, Paragraph, Clear};
        use ratatui::text::{Line, Span};

        // Calculate box size - add PRQL lines if mode 2
        let prql_lines: Vec<&str> = if info_mode == 2 && !prql.is_empty() {
            prql.split(" | ").collect()
        } else { vec![] };

        let max_desc_len = keys.iter().map(|(_, d)| d.len()).max().unwrap_or(10);
        let max_prql_len = prql_lines.iter().map(|s| s.len()).max().unwrap_or(0);
        let box_width = (max_desc_len.max(max_prql_len) + 11).min(60) as u16;
        let extra_lines = if prql_lines.is_empty() { 0 } else { prql_lines.len() + 1 };  // +1 for separator
        let box_height = (keys.len() + extra_lines + 2) as u16;

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
        let prql_style = Style::default().fg(RColor::Cyan);
        let mut lines: Vec<Line> = keys.iter().map(|(key, desc)| {
            Line::from(vec![
                Span::styled(format!("{:>5}", key), key_style),
                Span::raw(" "),
                Span::styled(*desc, text_style),
            ])
        }).collect();

        // Add PRQL if mode 2
        if !prql_lines.is_empty() {
            lines.push(Line::from(Span::styled("─────", Style::default().fg(RColor::DarkGray))));
            for pl in prql_lines {
                // Truncate long lines
                let s = if pl.len() > 55 { format!("{}…", &pl[..54]) } else { pl.to_string() };
                lines.push(Line::from(Span::styled(s, prql_style)));
            }
        }

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
    fn render_header_footer(frame: &mut Frame, table: &dyn Table, state: &TableState, xs: &[i32], screen_width: i32, row_num_width: u16, theme: &Theme, area: Rect, show_tabs: bool) {
        let row = area.height.saturating_sub(if show_tabs { 3 } else { 2 });
        if row == 0 { return; }
        let buf = frame.buffer_mut();
        let style = Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.header_fg));
        // Fill row
        for x in 0..area.width { buf[(x, row)].set_style(style); buf[(x, row)].set_char(' '); }
        let x_pos = if row_num_width > 0 { row_num_width + 1 } else { 0 };
        for col_idx in 0..table.cols() {
            let col_name = table.col_name(col_idx).unwrap_or_default();
            let x = xs.get(col_idx).copied().unwrap_or(0);
            let next_x = xs.get(col_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }
            let col_width = state.col_widths.get(col_idx).copied().unwrap_or(10) as usize;
            let start_x = x.max(0) as u16 + x_pos;
            let display = format!("{:width$}", col_name, width = col_width);
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

        let sel_info = format!(" [sel={}]", view.selected_cols.len());
        let left = if !message.is_empty() { format!("{}{}", message, sel_info) }
        else if matches!(view.kind, ViewKind::Freq | ViewKind::Meta) {
            // Show parent name and row count for Meta/Freq views
            let pn = view.parent.as_ref().map(|p| p.name.as_str()).unwrap_or("");
            let pr = view.parent.as_ref().map(|p| format!(" ({})", commify(&p.rows.to_string()))).unwrap_or_default();
            format!("{} <- {}{}{}", view.name, pn, pr, sel_info)
        }
        else { format!("{}{}", view.filename.as_deref().unwrap_or("(no file)"), sel_info) };

        // Use cached stats if column unchanged
        let col_stats_str = if view.cols() > 0 {
            let cc = view.state.cc;
            if let Some((cached_cc, ref s)) = view.cache.stats {
                if cached_cc == cc { s.clone() }
                else {
                    let s = view.col_stats(cc).format();
                    view.cache.stats = Some((cc, s.clone()));
                    s
                }
            } else {
                let s = view.col_stats(cc).format();
                view.cache.stats = Some((cc, s.clone()));
                s
            }
        } else { String::new() };

        let is_partial = matches!(view.source, ViewSource::Gz { partial: true, .. });
        let partial = if is_loading || is_partial { " Partial" } else { "" };
        let right = if col_stats_str.is_empty() { format!("{}/{}{}", view.state.cr, total_str, partial) }
        else { format!("{} {}/{}{}", col_stats_str, view.state.cr, total_str, partial) };

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
    pub fn test_format_cell(table: &dyn Table, col_idx: usize, row_idx: usize, decimals: usize) -> String {
        Self::format_cell(table, col_idx, row_idx, decimals)
    }

    #[cfg(test)]
    pub fn test_col_width(table: &dyn Table, col_idx: usize, state: &TableState, decimals: usize) -> u16 {
        Self::col_width(table, col_idx, state, decimals)
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
    use crate::table::{SimpleTable, ColType, Cell as TCell};
    use crate::state::TableState;

    #[test]
    fn test_null_not_commified() {
        let table = SimpleTable::new(
            vec!["int_col".into(), "float_col".into()],
            vec![ColType::Int, ColType::Float],
            vec![
                vec![TCell::Int(1000000), TCell::Float(1234.567)],
                vec![TCell::Null, TCell::Null],
                vec![TCell::Int(2000000), TCell::Float(9876.543)],
            ]
        );

        let int_null = Renderer::test_format_cell(&table, 0, 1, 3);
        assert_eq!(int_null, "null", "Integer null should be 'null', not 'n,ull'");

        let float_null = Renderer::test_format_cell(&table, 1, 1, 3);
        assert_eq!(float_null, "null", "Float null should be 'null', not 'n,ull'");

        let int_val = Renderer::test_format_cell(&table, 0, 0, 3);
        assert_eq!(int_val, "1,000,000", "Integer should be commified");

        let float_val = Renderer::test_format_cell(&table, 1, 0, 3);
        assert_eq!(float_val, "1,234.567", "Float should be commified");
    }

    #[test]
    fn test_last_col_width_not_extended() {
        // Last column should NOT fill rest of screen - looks weird for right-aligned numbers
        let table = SimpleTable::new(
            vec!["name".into(), "price".into()],
            vec![ColType::Str, ColType::Int],
            vec![
                vec![TCell::Str("apple".into()), TCell::Int(100)],
                vec![TCell::Str("banana".into()), TCell::Int(200)],
            ]
        );
        let state = TableState { viewport: (25, 120), ..Default::default() };

        // Get natural widths for both columns
        let name_w = Renderer::test_col_width(&table, 0, &state, 3);
        let price_w = Renderer::test_col_width(&table, 1, &state, 3);

        // name col: max("name".len(), "banana".len()) = 6
        assert!(name_w >= 6 && name_w <= 10, "name width {} should be ~6", name_w);
        // price col: max("price".len(), "200".len()) = 5
        assert!(price_w >= 5 && price_w <= 10, "price width {} should be ~5, not extended to fill screen", price_w);
    }
}
