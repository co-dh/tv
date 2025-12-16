//! Theme support - loads colors from cfg/themes.csv

use ratatui::crossterm::style::Color;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Clone)]
#[allow(dead_code)]  // reserved for future type-specific coloring
pub struct Theme {
    pub header_bg: Color,
    pub header_fg: Color,
    pub cursor_bg: Color,
    pub cursor_fg: Color,
    pub select_fg: Color,
    pub row_num_fg: Color,
    pub row_cur_fg: Color,
    pub string_fg: Color,
    pub number_fg: Color,
    pub null_fg: Color,
    pub info_border_fg: Color,
    pub info_key_fg: Color,
    pub status_bg: Color,
    pub status_fg: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            header_bg: Color::Rgb { r: 40, g: 40, b: 50 },
            header_fg: Color::White,
            cursor_bg: Color::Yellow,
            cursor_fg: Color::Black,
            select_fg: Color::Cyan,
            row_num_fg: Color::Magenta,
            row_cur_fg: Color::Yellow,
            string_fg: Color::Cyan,
            number_fg: Color::Magenta,
            null_fg: Color::DarkGrey,
            info_border_fg: Color::Cyan,
            info_key_fg: Color::Yellow,
            status_bg: Color::DarkGrey,
            status_fg: Color::White,
        }
    }
}

impl Theme {
    /// Load theme by name from cfg/themes.csv
    pub fn load(name: &str) -> Self {
        let themes = load_themes(Path::new("cfg/themes.csv"));
        themes.get(name).cloned().unwrap_or_default()
    }

    /// Load active theme from cfg/config.csv
    pub fn load_active() -> Self {
        let theme_name = load_config_value("theme").unwrap_or_else(|| "default".to_string());
        Self::load(&theme_name)
    }
}

/// Parse hex color #RRGGBB to Color::Rgb
fn parse_hex(s: &str) -> Option<Color> {
    let s = s.trim().trim_start_matches('#');
    if s.len() != 6 { return None; }
    let r = u8::from_str_radix(&s[0..2], 16).ok()?;
    let g = u8::from_str_radix(&s[2..4], 16).ok()?;
    let b = u8::from_str_radix(&s[4..6], 16).ok()?;
    Some(Color::Rgb { r, g, b })
}

/// Load all themes from CSV (long format: theme,name,color)
fn load_themes(path: &Path) -> HashMap<String, Theme> {
    let mut themes: HashMap<String, HashMap<String, Color>> = HashMap::new();
    let content = match fs::read_to_string(path) { Ok(c) => c, Err(_) => return HashMap::new() };

    // Parse long format: theme,name,color
    for line in content.lines().skip(1) {
        let p: Vec<&str> = line.split(',').collect();
        if p.len() >= 3 {
            if let Some(color) = parse_hex(p[2]) {
                themes.entry(p[0].to_string()).or_default().insert(p[1].to_string(), color);
            }
        }
    }

    // Convert to Theme structs
    themes.into_iter().map(|(name, colors)| {
        let get = |k: &str, def: Color| colors.get(k).copied().unwrap_or(def);
        let theme = Theme {
            header_bg: get("header_bg", Color::Rgb { r: 40, g: 40, b: 50 }),
            header_fg: get("header_fg", Color::White),
            cursor_bg: get("cursor_bg", Color::Yellow),
            cursor_fg: get("cursor_fg", Color::Black),
            select_fg: get("select_fg", Color::Cyan),
            row_num_fg: get("row_num_fg", Color::Magenta),
            row_cur_fg: get("row_cur_fg", Color::Yellow),
            string_fg: get("string_fg", Color::Cyan),
            number_fg: get("number_fg", Color::Magenta),
            null_fg: get("null_fg", Color::DarkGrey),
            info_border_fg: get("info_border_fg", Color::Cyan),
            info_key_fg: get("info_key_fg", Color::Yellow),
            status_bg: get("status_bg", Color::DarkGrey),
            status_fg: get("status_fg", Color::White),
        };
        (name, theme)
    }).collect()
}

/// Load a config value from cfg/config.csv
pub fn load_config_value(key: &str) -> Option<String> {
    let content = fs::read_to_string("cfg/config.csv").ok()?;
    for line in content.lines().skip(1) {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 2 && parts[0] == key {
            return Some(parts[1].to_string());
        }
    }
    None
}
