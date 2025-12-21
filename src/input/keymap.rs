#![allow(dead_code)]
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct KeyBinding {
    pub key: String,
    pub command: String,
}

#[derive(Debug)]
pub struct KeyMap {
    // tab -> command -> KeyBinding
    bindings: HashMap<String, HashMap<String, KeyBinding>>,
    // tab -> key -> command
    key_to_cmd: HashMap<String, HashMap<String, String>>,
}

impl Default for KeyMap {
    /// Create keymap from defaults + user overrides
    fn default() -> Self {
        let mut km = Self::from_defaults();
        // Try user override: ~/.config/tv/keys.csv
        if let Some(home) = std::env::var_os("HOME") {
            let path = Path::new(&home).join(".config/tv/keys.csv");
            if path.exists() { let _ = km.load_overrides(&path); }
        }
        km
    }
}

impl KeyMap {
    /// Default key bindings (tab, key, command)
    /// Key names follow Kakoune style: <ret>, <esc>, <space>, <up>, <c-x> etc.
    fn defaults() -> Vec<(&'static str, &'static str, &'static str)> {
        vec![
            // Common (all views)
            ("common", "q", "quit"),
            ("common", "<c-c>", "force_quit"),
            ("common", "<up>", "up"),
            ("common", "<down>", "down"),
            ("common", "<left>", "left"),
            ("common", "<right>", "right"),
            ("common", "h", "left"),
            ("common", "j", "down"),
            ("common", "k", "up"),
            ("common", "l", "right"),
            ("common", "<c-d>", "page_down"),
            ("common", "<c-u>", "page_up"),
            ("common", "g", "top"),
            ("common", "G", "bottom"),
            ("common", "<home>", "top"),
            ("common", "<end>", "bottom"),
            ("common", "I", "toggle_info"),
            ("common", ".", "decimals_inc"),
            ("common", ",", "decimals_dec"),
            ("common", "<space>", "toggle_sel"),
            ("common", "<esc>", "clear_sel"),
            ("common", "<backslash>", "filter"),
            ("common", "[", "sort"),
            ("common", "]", "sort-"),
            ("common", "F", "freq"),
            ("common", "M", "meta"),
            ("common", "D", "delete"),
            ("common", "T", "dup"),
            ("common", ":", "command"),
            ("common", "g", "goto_row"),
            ("common", "@", "goto_col"),
            ("common", "<ret>", "enter"),
            ("common", "L", "from"),
            ("common", "S", "swap"),
            ("common", "s", "select_cols"),
            // l is now right movement (vim hjkl)
            ("common", "r", "lr"),
            ("common", "C", "corr"),
            ("common", "^", "rename"),
            ("common", "c", "derive"),
            ("common", "$", "convert"),
            ("common", "b", "aggregate"),
            ("common", "!", "xkey"),
            ("common", "P", "pivot"),
            // Freq view - override enter to filter parent
            ("freq", "<ret>", "filter_parent"),
            // Meta view - enter triggers xkey on selected cols
            ("meta", "<ret>", "enter"),
            ("meta", "D", "delete_sel"),
            ("meta", "0", "sel_null"),
            ("meta", "1", "sel_single"),
            // Corr view
            ("corr", "<ret>", "goto_col"),
        ]
    }


    /// Build keymap from default bindings
    fn from_defaults() -> Self {
        let mut bindings: HashMap<String, HashMap<String, KeyBinding>> = HashMap::new();
        let mut key_to_cmd: HashMap<String, HashMap<String, String>> = HashMap::new();
        for (tab, key, cmd) in Self::defaults() {
            let binding = KeyBinding { key: key.to_string(), command: cmd.to_string() };
            bindings.entry(tab.to_string()).or_default().insert(cmd.to_string(), binding);
            key_to_cmd.entry(tab.to_string()).or_default().insert(key.to_string(), cmd.to_string());
        }
        Self { bindings, key_to_cmd }
    }

    /// Load overrides from CSV file (tab,key,command)
    fn load_overrides(&mut self, path: &Path) -> anyhow::Result<()> {
        let content = fs::read_to_string(path)?;
        for line in content.lines().skip(1) {
            let parts: Vec<&str> = line.splitn(3, ',').collect();
            if parts.len() >= 3 {
                let (tab, key, cmd) = (parts[0], parts[1], parts[2]);
                let binding = KeyBinding { key: key.to_string(), command: cmd.to_string() };
                self.bindings.entry(tab.to_string()).or_default().insert(cmd.to_string(), binding);
                self.key_to_cmd.entry(tab.to_string()).or_default().insert(key.to_string(), cmd.to_string());
            }
        }
        Ok(())
    }

    /// Load keymap from CSV file (for backwards compat)
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let mut bindings: HashMap<String, HashMap<String, KeyBinding>> = HashMap::new();
        let mut key_to_cmd: HashMap<String, HashMap<String, String>> = HashMap::new();
        for line in content.lines().skip(1) {
            let parts: Vec<&str> = line.splitn(4, ',').collect();
            if parts.len() >= 3 {
                let (tab, key, cmd) = (parts[0].to_string(), parts[1].to_string(), parts[2].to_string());
                if let Some(existing) = key_to_cmd.get(&tab).and_then(|m| m.get(&key)) {
                    return Err(anyhow::anyhow!("Key conflict: '{}' mapped to both '{}' and '{}'", key, existing, cmd));
                }
                let binding = KeyBinding { key: key.clone(), command: cmd.clone() };
                bindings.entry(tab.clone()).or_default().insert(cmd.clone(), binding);
                key_to_cmd.entry(tab).or_default().insert(key, cmd);
            }
        }
        Ok(Self { bindings, key_to_cmd })
    }

    /// Get command for a key in given tab (checks tab first, then table, then common)
    pub fn get_command(&self, tab: &str, key: &str) -> Option<&str> {
        // Check specific tab first
        if let Some(cmds) = self.key_to_cmd.get(tab) {
            if let Some(cmd) = cmds.get(key) { return Some(cmd); }
        }
        // Fall back to table (all views inherit table keys)
        if tab != "table" {
            if let Some(cmds) = self.key_to_cmd.get("table") {
                if let Some(cmd) = cmds.get(key) { return Some(cmd); }
            }
        }
        // Fall back to common
        self.key_to_cmd.get("common").and_then(|m| m.get(key)).map(|s| s.as_str())
    }

    /// Get key for a command in given tab (checks tab first, then common)
    pub fn get_key(&self, tab: &str, command: &str) -> Option<&str> {
        if let Some(cmds) = self.bindings.get(tab) {
            if let Some(binding) = cmds.get(command) {
                return Some(&binding.key);
            }
        }
        if let Some(cmds) = self.bindings.get("common") {
            if let Some(binding) = cmds.get(command) {
                return Some(&binding.key);
            }
        }
        None
    }

    /// Get hint text for a command (hint, category)
    /// Categories: 0=view-specific, 1=selection, 2=filter, 3=data, 4=file, 5=display, 9=nav (hidden)
    pub fn hint(command: &str) -> Option<(&'static str, u8)> {
        match command {
            // Navigation - category 9 (hidden from info box)
            "quit" => Some(("quit", 5)),
            "top" | "bottom" | "page_down" | "page_up" => None,  // hide nav
            // Selection
            "toggle_sel" => Some(("sel", 1)),
            "select_cols" => Some(("sel col", 3)),
            "clear_sel" => Some(("clr sel", 1)),
            "sel_null" => Some(("sel null", 0)),
            "sel_single" => Some(("sel single", 0)),
            // Filter
            "filter" => Some(("filter", 2)),
            // Data ops
            "freq" => Some(("freq", 3)),
            "meta" => Some(("meta", 3)),
            "corr" => Some(("corr", 3)),
            "sort" => Some(("sort↑", 3)),
            "sort-" => Some(("sort↓", 3)),
            "delete" => Some(("del", 3)),
            "delete_sel" => Some(("del sel", 3)),
            "filter_parent" => Some(("filter↑", 0)),
            // File ops
            "from" => Some(("load", 4)),
            "save" => Some(("save", 4)),
            // Transform
            "xkey" => Some(("xkey", 3)),
            // Display
            "decimals_inc" => Some(("dec++", 5)),
            "decimals_dec" => Some(("dec--", 5)),
            "command" => Some(("cmd", 5)),
            _ => None,
        }
    }

    /// Get hints for info box for a tab (sorted by category: view-specific, selection, search, data, file, display)
    pub fn get_hints(&self, tab: &str) -> Vec<(String, &'static str)> {
        let mut hints: Vec<(String, &'static str, u8)> = Vec::new();  // (key, text, category)
        let mut seen_cmds = std::collections::HashSet::new();

        // Collect commands (tab-specific first, then common)
        for t in [tab, "common"] {
            if let Some(cmds) = self.bindings.get(t) {
                for (cmd, binding) in cmds {
                    if !seen_cmds.contains(cmd) {
                        if let Some((text, cat)) = Self::hint(cmd) {
                            hints.push((binding.key.clone(), text, cat));
                            seen_cmds.insert(cmd.clone());
                        }
                    }
                }
            }
        }

        // Add view-specific hints
        match tab {
            "table" => hints.push(("<ret>".to_string(), "sel+edit", 0)),
            "folder" => {
                hints.push(("<ret>".to_string(), "open", 0));
                hints.push(("D".to_string(), "del file", 0));
            }
            _ => {}
        }

        // Sort by category, then by hint text
        hints.sort_by(|a, b| a.2.cmp(&b.2).then(a.1.cmp(&b.1)));

        // Strip category
        hints.into_iter().map(|(k, h, _)| (k, h)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let km = KeyMap::from_defaults();
        // Common keys
        assert_eq!(km.get_command("table", "q"), Some("quit"));
        assert_eq!(km.get_command("table", "<up>"), Some("up"));
        // Table keys
        assert_eq!(km.get_command("table", "F"), Some("freq"));
        assert_eq!(km.get_command("table", "["), Some("sort"));
        assert_eq!(km.get_command("table", "<ret>"), Some("enter"));
        // Freq view
        assert_eq!(km.get_command("freq", "<ret>"), Some("filter_parent"));
        // Freq inherits table keys
        assert_eq!(km.get_command("freq", "["), Some("sort"));
        assert_eq!(km.get_command("freq", "q"), Some("quit"));
        // Folder has its own sort/freq bindings
        assert_eq!(km.get_command("folder", "["), Some("sort"));
        assert_eq!(km.get_command("folder", "F"), Some("freq"));
        assert_eq!(km.get_command("folder", "<ret>"), Some("enter"));
    }

    #[test]
    fn test_get_key() {
        let km = KeyMap::from_defaults();
        assert_eq!(km.get_key("table", "freq"), Some("F"));
        assert_eq!(km.get_key("freq", "filter_parent"), Some("<ret>"));
        assert_eq!(km.get_key("common", "quit"), Some("q"));
    }
}
