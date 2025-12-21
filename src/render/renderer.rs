use crate::app::AppContext;
use crate::data::dynload;
use crate::data::table::{Cell, ColType, Table};
use crate::utils::commify;
use crate::state::{TableState, ViewState};
use crate::util::theme::Theme;
use ratatui::prelude::*;
use ratatui::style::{Color as RColor, Modifier, Style};
use ratatui::widgets::Tabs;
use std::collections::HashSet;

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
            let show_tabs = true;  // always show tab bar
            Self::render_table(frame, view, area, &selected_cols, &selected_rows, decimals, &theme, show_tabs);
            if info_mode > 0 {
                Self::render_info_box(frame, &view_name, stack_len, area, &hints, &theme, info_mode, &prql);
            }
            Self::render_tabs(frame, &stack_names, area, &theme);
            Self::render_status_bar(frame, view, &message, is_loading, area, &theme);
        } else {
            Self::empty_msg(frame, &message, area);
            if info_mode > 0 {
                Self::render_info_box(frame, "table", stack_len, area, &hints, &theme, info_mode, "");
            }
        }
    }

    /// Render table data
    pub fn render_table(frame: &mut Frame, view: &mut ViewState, area: Rect, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, decimals: usize, theme: &Theme, show_tabs: bool) {
        // Fetch data via plugin using PRQL (compiled to SQL)
        // Round to chunk boundaries to maximize cache hits when scrolling
        const CHUNK: usize = 1000;
        let (rows_needed, r0) = (area.height as usize + 100, view.state.r0);
        let chunk_start = (r0 / CHUNK) * CHUNK;  // round down to chunk
        let chunk_end = chunk_start + CHUNK.max(rows_needed);  // at least 1 chunk
        let path = view.path.clone();
        let prql = view.prql.clone();
        let lazy_offset = path.as_ref().and_then(|p| {
            let plugin = dynload::get_for(p)?;
            // PRQL: take start..end (1-based, positive range required)
            let q = format!("{} | take {}..{}", prql, chunk_start + 1, chunk_end + 1);
            let t = plugin.query(&q, p)?;
            view.data = dynload::to_box_table(&t);
            Some(chunk_start)
        }).unwrap_or(0);

        // Use Table trait for polars-free rendering
        let table = view.data.as_ref();
        // Get total rows via plugin (caches it)
        let total_rows = view.total_rows();
        let is_correlation = view.name.starts_with("corr");
        let dcols = view.display_cols();  // column indices in display order

        // Calculate column widths (last displayed col uses actual content width)
        if view.state.need_widths() {
            let last_col = dcols.last().copied().unwrap_or(usize::MAX);
            let widths: Vec<u16> = (0..table.cols())
                .map(|c| Self::col_width(table, c, &view.state, decimals, c == last_col))
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

        // Row number column removed - use goto_row for navigation
        let row_num_width = 0u16;
        let screen_width = area.width.saturating_sub(if row_num_width > 0 { row_num_width + 1 } else { 0 }) as i32;

        // Calculate xs - x position for each column in display order
        let mut xs: Vec<i32> = Vec::with_capacity(dcols.len() + 1);
        xs.push(0);
        for &data_idx in &dcols {
            let col_width = state.col_widths.get(data_idx).copied().unwrap_or(10) as i32;
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
        Self::render_headers_xs(frame, table, state, &dcols, &xs, screen_width, row_num_width, selected_cols, col_sep, theme, area);

        // Render data rows (for lazy parquet, df_idx = row_idx - lazy_offset)
        for row_idx in state.r0..end_row {
            let df_idx = row_idx - lazy_offset;
            if df_idx >= table.rows() { break; }  // fetched window exhausted
            let screen_row = (row_idx - state.r0 + 1) as u16;
            Self::render_row_xs(frame, table, df_idx, row_idx, state, &dcols, &xs, screen_width, row_num_width, is_correlation, selected_cols, selected_rows, col_sep, decimals, theme, area, screen_row);
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
                        if px < area.width { buf[(px, y)].set_char('|').set_style(sep_style); }
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
        Self::render_header_footer(frame, table, state, &dcols, &xs, screen_width, row_num_width, theme, area, show_tabs);
    }

    /// Render column headers (dcols = display order of data column indices)
    fn render_headers_xs(frame: &mut Frame, table: &dyn Table, state: &TableState, dcols: &[usize], xs: &[i32], screen_width: i32, row_num_width: u16, selected_cols: &HashSet<usize>, _col_sep: Option<usize>, theme: &Theme, area: Rect) {
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

        for (disp_idx, &data_idx) in dcols.iter().enumerate() {
            let col_name = table.col_name(data_idx).unwrap_or_default();
            let x = xs[disp_idx];
            let next_x = xs.get(disp_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_current = disp_idx == state.cc;
            let is_selected = selected_cols.contains(&disp_idx);
            let col_width = state.col_widths.get(data_idx).copied().unwrap_or(10) as usize;

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

    /// Render a single data row (dcols = display order of data column indices)
    fn render_row_xs(frame: &mut Frame, table: &dyn Table, df_idx: usize, row_idx: usize, state: &TableState, dcols: &[usize], xs: &[i32], screen_width: i32, row_num_width: u16, is_correlation: bool, selected_cols: &HashSet<usize>, selected_rows: &HashSet<usize>, _col_sep: Option<usize>, decimals: usize, theme: &Theme, area: Rect, screen_row: u16) {
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

        for (disp_idx, &data_idx) in dcols.iter().enumerate() {
            let x = xs[disp_idx];
            let next_x = xs.get(disp_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }

            let is_cur_col = disp_idx == state.cc;
            let is_cur_cell = is_cur_row && is_cur_col;
            let is_sel = selected_cols.contains(&disp_idx);

            let col_width = state.col_widths.get(data_idx).copied().unwrap_or(10) as usize;
            let value = Self::format_cell(table, data_idx, df_idx, decimals);

            // Correlation color
            let corr_color = if is_correlation && disp_idx > 0 { Self::correlation_color(&value) } else { None };

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
            // Right-align numeric columns
            let is_num = table.col_type(data_idx).is_numeric();
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
    /// is_last: last displayed column uses actual content width (no limit)
    fn col_width(table: &dyn Table, col_idx: usize, state: &TableState, decimals: usize, is_last: bool) -> u16 {
        const MIN_WIDTH: usize = 3;
        let col_name = table.col_name(col_idx).unwrap_or_default();
        // Last col: no limit. Text cols: 80. Numeric: 30. Path: 80.
        let max_limit = if is_last { usize::MAX }
            else if col_name == "path" { 80 }
            else if table.col_type(col_idx) == ColType::Str { 80 }
            else { 30 };

        let mut max_w = col_name.len();
        let sample = ((state.viewport.0.saturating_sub(2) as usize) * 3).max(100);
        let start = state.cr.saturating_sub(sample / 2);
        let end = (start + sample).min(table.rows());

        for r in start..end {
            let v = Self::format_cell(table, col_idx, r, decimals);
            max_w = max_w.max(v.len());
            if max_w >= max_limit { break; }
        }
        max_w.max(MIN_WIDTH).min(max_limit) as u16
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

        // Mode 3: show system commands from plugin
        if info_mode == 3 {
            Self::render_commands_box(frame, stack_len, area, theme);
            return;
        }

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
        let box_y = area.height.saturating_sub(box_height + 2);  // +2 to clear status line
        let box_area = Rect::new(box_x, box_y, box_width, box_height);

        // Clear area first
        frame.render_widget(Clear, box_area);

        // Block with border and title
        let title = if stack_len > 1 { format!(" [#{}] ", stack_len) } else { " [tv] ".to_string() };
        let border_style = Style::default().fg(to_rcolor(theme.info_border_fg)).bg(RColor::Black);
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .style(Style::default().bg(RColor::Black))
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

    /// Render system commands box (mode 3) - fetches from sqlite plugin
    fn render_commands_box(frame: &mut Frame, stack_len: usize, area: Rect, theme: &Theme) {
        use ratatui::widgets::{Block, Borders, Paragraph, Clear};
        use ratatui::text::{Line, Span};

        // Fetch commands from sqlite plugin
        let cmds: Vec<(String, String)> = dynload::get_sqlite()
            .and_then(|p| p.query("from df", "source:commands"))
            .map(|t| (0..t.rows()).map(|r| (t.cell(r, 0).format(10), t.cell(r, 1).format(10))).collect())
            .unwrap_or_default();

        let box_width = 35u16;
        let box_height = (cmds.len() + 2).min(20) as u16;
        let box_x = area.width.saturating_sub(box_width + 1);
        let box_y = area.height.saturating_sub(box_height + 2);  // +2 to clear status line
        let box_area = Rect::new(box_x, box_y, box_width, box_height);

        frame.render_widget(Clear, box_area);

        let title = if stack_len > 1 { format!(" [#{}] :cmd ", stack_len) } else { " :commands ".to_string() };
        let border_style = Style::default().fg(to_rcolor(theme.info_border_fg)).bg(RColor::Black);
        let block = Block::default().borders(Borders::ALL).border_style(border_style).style(Style::default().bg(RColor::Black)).title(title);

        let cmd_style = Style::default().fg(to_rcolor(theme.info_key_fg));
        let desc_style = Style::default().fg(RColor::White);
        let lines: Vec<Line> = cmds.iter().take(18).map(|(cmd, desc)| {
            Line::from(vec![
                Span::styled(format!("{:>15}", cmd), cmd_style),
                Span::raw(" "),
                Span::styled(desc.as_str(), desc_style),
            ])
        }).collect();

        let para = Paragraph::new(lines).block(block);
        frame.render_widget(para, box_area);
    }

    /// Render view stack as tabs (names are commands like "from file.csv", "freq a")
    fn render_tabs(frame: &mut Frame, names: &[String], area: Rect, theme: &Theme) {
        let row = area.height - 2;
        let tab_area = Rect::new(0, row, area.width, 1);
        // Shorten: truncate to 25 chars (full command shown, optionally shorten path)
        let short: Vec<String> = names.iter().map(|s| {
            if s.len() > 25 { format!("{}…", &s[..24]) } else { s.clone() }
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
            .divider("|");
        frame.render_widget(tabs, tab_area);
    }

    /// Render column header above tabs/status (footer header) - aligned with table
    fn render_header_footer(frame: &mut Frame, table: &dyn Table, state: &TableState, dcols: &[usize], xs: &[i32], screen_width: i32, row_num_width: u16, theme: &Theme, area: Rect, show_tabs: bool) {
        let row = area.height.saturating_sub(if show_tabs { 3 } else { 2 });
        if row == 0 { return; }
        let buf = frame.buffer_mut();
        let style = Style::default().bg(to_rcolor(theme.header_bg)).fg(to_rcolor(theme.header_fg));
        // Fill row
        for x in 0..area.width { buf[(x, row)].set_style(style); buf[(x, row)].set_char(' '); }
        let x_pos = if row_num_width > 0 { row_num_width + 1 } else { 0 };
        for (disp_idx, &data_idx) in dcols.iter().enumerate() {
            let col_name = table.col_name(data_idx).unwrap_or_default();
            let x = xs.get(disp_idx).copied().unwrap_or(0);
            let next_x = xs.get(disp_idx + 1).copied().unwrap_or(x);
            if next_x <= 0 { continue; }
            if x >= screen_width { break; }
            let col_width = state.col_widths.get(data_idx).copied().unwrap_or(10) as usize;
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

        // Show total rows via plugin
        let total_str = commify(&view.total_rows().to_string());

        let keys = view.col_separator.unwrap_or(0);
        let sel_info = if keys > 0 {
            format!(" [keys={} sel={}]", keys, view.selected_cols.len())
        } else {
            format!(" [sel={}]", view.selected_cols.len())
        };
        let left = if !message.is_empty() { format!("{}{}", message, sel_info) }
        else if view.is_row_sel() {
            // Show parent name and row count for Meta/Freq views
            let pn = view.parent.as_ref().map(|p| p.name.as_str()).unwrap_or("");
            let pr = view.parent.as_ref().map(|p| format!(" ({})", commify(&p.rows.to_string()))).unwrap_or_default();
            format!("{} <- {}{}{}", view.name, pn, pr, sel_info)
        }
        else { format!("{}{}", view.path.as_deref().unwrap_or("(no file)"), sel_info) };

        // Get column stats for current column (via plugin)
        let col_stats_str = if view.cols() > 0 {
            view.col_stats_plugin(view.state.cc).format()
        } else { String::new() };

        let partial = if is_loading || view.partial { " Partial" } else { "" };
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
    pub fn test_col_width(table: &dyn Table, col_idx: usize, state: &TableState, decimals: usize, is_last: bool) -> u16 {
        Self::col_width(table, col_idx, state, decimals, is_last)
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
    use crate::data::table::{SimpleTable, ColType, Cell as TCell};
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

        // Get natural widths for both columns (price is last)
        let name_w = Renderer::test_col_width(&table, 0, &state, 3, false);
        let price_w = Renderer::test_col_width(&table, 1, &state, 3, true);

        // name col: max("name".len(), "banana".len()) = 6
        assert!(name_w >= 6 && name_w <= 10, "name width {} should be ~6", name_w);
        // price col: max("price".len(), "200".len()) = 5 (last col uses actual width)
        assert!(price_w >= 5 && price_w <= 10, "price width {} should be ~5", price_w);
    }

    #[test]
    fn test_render_folder_sort() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use crate::state::ViewState;
        use std::collections::HashSet;
        use crate::util::theme::Theme;

        // Load sqlite plugin for test
        let _ = crate::data::dynload::load_sqlite("./target/release/libtv_sqlite.so");

        // Create folder data (unsorted)
        let table = SimpleTable::new(
            vec!["path".into(), "size".into()],
            vec![ColType::Str, ColType::Int],
            vec![
                vec![TCell::Str("b.csv".into()), TCell::Int(200)],
                vec![TCell::Str("a.csv".into()), TCell::Int(100)],
                vec![TCell::Str("c.csv".into()), TCell::Int(50)],
            ]
        );

        // Create view with sort by size ascending (use unique ID to avoid test conflicts)
        let mut view = ViewState::build(100, "folder:test").data(Box::new(table)).register();
        view.prql = "from df | sort {size}".to_string();

        // Check plugin is loaded and path is set
        assert!(view.path.is_some(), "path should be set: {:?}", view.path);

        // Render to test backend
        let backend = TestBackend::new(80, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| {
            let area = frame.area();
            Renderer::render_table(
                frame, &mut view, area,
                &HashSet::new(), &HashSet::new(), 3,
                &Theme::default(), false
            );
        }).unwrap();

        // Check buffer contains sorted data (c.csv with size 50 should be first)
        let buf = terminal.backend().buffer().clone();
        let content = (0..buf.area.height)
            .map(|y| (0..buf.area.width).map(|x| buf.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' ')).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n");

        // After sort by size asc: c.csv(50), a.csv(100), b.csv(200)
        let c_pos = content.find("c.csv");
        let a_pos = content.find("a.csv");
        let b_pos = content.find("b.csv");
        assert!(c_pos.is_some() && a_pos.is_some() && b_pos.is_some(), "All files should be in output: {}", content);
        assert!(c_pos < a_pos && a_pos < b_pos, "Should be sorted by size: c < a < b, got c={:?} a={:?} b={:?}\n{}", c_pos, a_pos, b_pos, content);
    }

    #[test]
    fn test_sort_command_flow() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use crate::state::ViewState;
        use crate::command::transform::Sort;
        use crate::command::Command;
        use crate::app::AppContext;
        use std::collections::HashSet;
        use crate::util::theme::Theme;

        // Load sqlite plugin
        let _ = crate::data::dynload::load_sqlite("./target/release/libtv_sqlite.so");

        // Create folder data (unsorted: b=200, a=100, c=50)
        let table = SimpleTable::new(
            vec!["path".into(), "size".into()],
            vec![ColType::Str, ColType::Int],
            vec![
                vec![TCell::Str("b.csv".into()), TCell::Int(200)],
                vec![TCell::Str("a.csv".into()), TCell::Int(100)],
                vec![TCell::Str("c.csv".into()), TCell::Int(50)],
            ]
        );

        // Create app with folder view
        let mut app = AppContext::default();
        app.stack.push(ViewState::build(200, "folder:test").data(Box::new(table)).register());

        // Move cursor to size column
        if let Some(v) = app.view_mut() { v.state.cc = 1; }

        // Execute Sort command (like pressing [)
        let mut sort = Sort { col_name: "size".to_string(), descending: false };
        sort.exec(&mut app).unwrap();

        // Verify prql changed
        assert!(app.view().unwrap().prql.contains("sort"), "prql should contain sort: {}", app.view().unwrap().prql);

        // Render
        let backend = TestBackend::new(80, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| {
            let area = frame.area();
            let view = app.view_mut().unwrap();
            Renderer::render_table(frame, view, area, &HashSet::new(), &HashSet::new(), 3, &Theme::default(), false);
        }).unwrap();

        // Check sorted order: c.csv(50) < a.csv(100) < b.csv(200)
        let buf = terminal.backend().buffer().clone();
        let content = (0..buf.area.height)
            .map(|y| (0..buf.area.width).map(|x| buf.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' ')).collect::<String>())
            .collect::<Vec<_>>().join("\n");

        let c_pos = content.find("c.csv");
        let a_pos = content.find("a.csv");
        let b_pos = content.find("b.csv");
        eprintln!("Buffer:\n{}", content);
        assert!(c_pos.is_some() && a_pos.is_some() && b_pos.is_some(), "All files should be in output");
        assert!(c_pos < a_pos && a_pos < b_pos, "Should be sorted: c < a < b, got c={:?} a={:?} b={:?}", c_pos, a_pos, b_pos);
    }

    #[test]
    fn test_multi_render_sort() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use crate::state::ViewState;
        use crate::command::transform::Sort;
        use crate::command::Command;
        use crate::app::AppContext;
        use std::collections::HashSet;
        use crate::util::theme::Theme;

        // Load sqlite plugin
        let _ = crate::data::dynload::load_sqlite("./target/release/libtv_sqlite.so");

        // Create folder data (unsorted: b=200, a=100, c=50)
        let table = SimpleTable::new(
            vec!["path".into(), "size".into()],
            vec![ColType::Str, ColType::Int],
            vec![
                vec![TCell::Str("b.csv".into()), TCell::Int(200)],
                vec![TCell::Str("a.csv".into()), TCell::Int(100)],
                vec![TCell::Str("c.csv".into()), TCell::Int(50)],
            ]
        );

        let mut app = AppContext::default();
        // Use unique ID to avoid test conflicts with other tests
        app.stack.push(ViewState::build(300, "folder:test").data(Box::new(table)).register());

        let backend = TestBackend::new(80, 10);
        let mut terminal = Terminal::new(backend).unwrap();

        // First render (before sort) - should be: b, a, c
        terminal.draw(|frame| {
            let view = app.view_mut().unwrap();
            Renderer::render_table(frame, view, frame.area(), &HashSet::new(), &HashSet::new(), 3, &Theme::default(), false);
        }).unwrap();

        let buf1 = terminal.backend().buffer().clone();
        let content1 = (0..3).map(|y| (0..20).map(|x| buf1.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' ')).collect::<String>()).collect::<Vec<_>>().join("\n");
        eprintln!("Before sort:\n{}", content1);

        // Execute Sort
        let mut sort = Sort { col_name: "size".to_string(), descending: false };
        sort.exec(&mut app).unwrap();
        eprintln!("After sort, prql: {}", app.view().unwrap().prql);

        // Second render (after sort) - should be: c, a, b
        terminal.draw(|frame| {
            let view = app.view_mut().unwrap();
            Renderer::render_table(frame, view, frame.area(), &HashSet::new(), &HashSet::new(), 3, &Theme::default(), false);
        }).unwrap();

        let buf2 = terminal.backend().buffer().clone();
        let content2 = (0..4).map(|y| (0..20).map(|x| buf2.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' ')).collect::<String>()).collect::<Vec<_>>().join("\n");
        eprintln!("After sort:\n{}", content2);

        // Verify sorted order
        let c_pos = content2.find("c.csv");
        let a_pos = content2.find("a.csv");
        let b_pos = content2.find("b.csv");
        assert!(c_pos < a_pos && a_pos < b_pos, "After sort should be c < a < b");
    }

    #[test]
    fn test_no_quotes_in_strings() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use crate::state::ViewState;
        use crate::app::AppContext;
        use std::collections::HashSet;
        use crate::util::theme::Theme;

        // Load sqlite plugin
        let _ = crate::data::dynload::load_sqlite("./target/release/libtv_sqlite.so");

        // Create data with string values
        let table = SimpleTable::new(
            vec!["name".into(), "value".into()],
            vec![ColType::Str, ColType::Str],
            vec![
                vec![TCell::Str("hello".into()), TCell::Str("world".into())],
                vec![TCell::Str("foo".into()), TCell::Str("bar".into())],
            ]
        );

        let mut app = AppContext::default();
        app.stack.push(ViewState::build(10000, "test").data(Box::new(table)).register());

        let backend = TestBackend::new(80, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| {
            let view = app.view_mut().unwrap();
            Renderer::render_table(frame, view, frame.area(), &HashSet::new(), &HashSet::new(), 3, &Theme::default(), false);
        }).unwrap();

        let buf = terminal.backend().buffer().clone();
        let content = (0..5).map(|y| (0..30).map(|x| buf.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' ')).collect::<String>()).collect::<Vec<_>>().join("\n");
        eprintln!("Content:\n{}", content);

        // Should NOT have quotes around strings
        assert!(!content.contains("\"hello\""), "Should not have quotes: {}", content);
        assert!(!content.contains("\"world\""), "Should not have quotes: {}", content);
        assert!(content.contains("hello"), "Should have hello: {}", content);
        assert!(content.contains("world"), "Should have world: {}", content);
    }

    #[test]
    fn test_scroll_down_moves_content() {
        // Verify scrolling down past screen moves content up
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use crate::state::ViewState;
        use crate::app::AppContext;
        use std::collections::HashSet;
        use crate::util::theme::Theme;

        // Create table with 20 rows (more than screen height)
        let rows: Vec<Vec<TCell>> = (0..20).map(|i| vec![
            TCell::Str(format!("row{:02}", i)),
            TCell::Int(i * 10),
        ]).collect();
        let table = SimpleTable::new(
            vec!["name".into(), "val".into()],
            vec![ColType::Str, ColType::Int],
            rows,
        );

        let mut app = AppContext::default();
        app.stack.push(ViewState::build(10001, "test").data(Box::new(table)).register());

        let backend = TestBackend::new(40, 15);  // 15 rows high - enough for header/footer/data
        let mut terminal = Terminal::new(backend).unwrap();

        // Render at row 0 - should see row00, row01, etc.
        terminal.draw(|frame| {
            let view = app.view_mut().unwrap();
            view.state.cr = 0;
            view.state.viewport = (15, 40);
            Renderer::render_table(frame, view, frame.area(), &HashSet::new(), &HashSet::new(), 3, &Theme::default(), false);
        }).unwrap();

        let buf1 = terminal.backend().buffer().clone();
        let content1 = (0..15).map(|y| (0..20).map(|x| buf1.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' ')).collect::<String>()).collect::<Vec<_>>().join("\n");
        eprintln!("At row 0:\n{}", content1);

        assert!(content1.contains("row00"), "Should see row00 at top");
        assert!(content1.contains("row01"), "Should see row01");
        assert!(!content1.contains("row15"), "Should NOT see row15 yet");

        // Scroll to row 15 - should see row15, not row00
        terminal.draw(|frame| {
            let view = app.view_mut().unwrap();
            view.state.cr = 15;
            view.state.visible();  // update r0 to show cursor
            Renderer::render_table(frame, view, frame.area(), &HashSet::new(), &HashSet::new(), 3, &Theme::default(), false);
        }).unwrap();

        let buf2 = terminal.backend().buffer().clone();
        let content2 = (0..15).map(|y| (0..20).map(|x| buf2.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' ')).collect::<String>()).collect::<Vec<_>>().join("\n");
        eprintln!("At row 15:\n{}", content2);

        assert!(content2.contains("row15"), "Should see row15 after scroll");
        assert!(!content2.contains("row00"), "row00 should be scrolled off");
    }

    #[test]
    fn test_col_stats_with_parquet() {
        use crate::command::io::From;
        use crate::command::Command;
        use crate::app::AppContext;

        // Load plugins
        let _ = crate::data::dynload::load_polars("./target/release/libtv_polars.so");
        let _ = crate::data::dynload::load_sqlite("./target/release/libtv_sqlite.so");

        let mut app = AppContext::default();
        let mut from_cmd = From { file_path: "tests/data/sample.parquet".to_string() };
        from_cmd.exec(&mut app).unwrap();

        let view = app.view().unwrap();
        // age column is at index 1
        let stats = view.col_stats_plugin(1);

        // sample.parquet has 10000 rows, age should have reasonable stats
        // min should be around 18, max around 80, avg around 49
        assert!(!stats.min.is_empty(), "min should not be empty");
        assert!(!stats.max.is_empty(), "max should not be empty");
        assert!(!stats.median.is_empty(), "avg should not be empty");
        assert!(!stats.sigma.is_empty(), "stddev should not be empty");

        // Verify stats are from full dataset (not just first 50 rows)
        let min: f64 = stats.min.parse().unwrap_or(0.0);
        let max: f64 = stats.max.parse().unwrap_or(0.0);
        assert!(min >= 15.0 && min <= 25.0, "min age should be ~18-20, got {}", min);
        assert!(max >= 70.0 && max <= 85.0, "max age should be ~80, got {}", max);
    }
}
