#![allow(dead_code)]
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct KeyBinding {
    pub key: String,
    pub command: String,
    pub description: String,
}

#[derive(Debug)]
pub struct KeyMap {
    // tab -> command -> KeyBinding
    bindings: HashMap<String, HashMap<String, KeyBinding>>,
    // tab -> key -> command
    key_to_cmd: HashMap<String, HashMap<String, String>>,
}

impl KeyMap {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let mut bindings: HashMap<String, HashMap<String, KeyBinding>> = HashMap::new();
        let mut key_to_cmd: HashMap<String, HashMap<String, String>> = HashMap::new();

        for line in content.lines().skip(1) {
            let parts: Vec<&str> = line.splitn(4, ',').collect();
            if parts.len() >= 3 {
                let tab = parts[0].to_string();
                let key = parts[1].to_string();
                let command = parts[2].to_string();
                let description = parts.get(3).unwrap_or(&"").to_string();

                // Check for key conflict: same tab+key mapped to different command
                if let Some(existing) = key_to_cmd.get(&tab).and_then(|m| m.get(&key)) {
                    return Err(anyhow::anyhow!(
                        "Key conflict in {}: '{}' mapped to both '{}' and '{}'",
                        path.display(), key, existing, command
                    ));
                }

                let binding = KeyBinding { key: key.clone(), command: command.clone(), description };

                bindings.entry(tab.clone()).or_default().insert(command.clone(), binding);
                key_to_cmd.entry(tab).or_default().insert(key, command);
            }
        }

        Ok(Self { bindings, key_to_cmd })
    }

    /// Get command for a key in given tab (checks tab first, then common)
    pub fn get_command(&self, tab: &str, key: &str) -> Option<&str> {
        if let Some(cmds) = self.key_to_cmd.get(tab) {
            if let Some(cmd) = cmds.get(key) {
                return Some(cmd);
            }
        }
        if let Some(cmds) = self.key_to_cmd.get("common") {
            if let Some(cmd) = cmds.get(key) {
                return Some(cmd);
            }
        }
        None
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
    /// Categories: 0=view-specific, 1=selection, 2=search, 3=data, 4=file, 5=display, 9=nav (hidden)
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
            // Search
            "search" => Some(("search", 2)),
            "filter" => Some(("filter", 2)),
            "next_match" => Some(("next", 2)),
            "prev_match" => Some(("prev", 2)),
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
            "table" => hints.push(("Enter".to_string(), "sel+edit", 0)),
            "folder" => {
                hints.push(("Enter".to_string(), "open", 0));
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

impl Default for KeyMap {
    fn default() -> Self {
        Self {
            bindings: HashMap::new(),
            key_to_cmd: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_key_conflict_detection() {
        let tmp = std::env::temp_dir().join("tv_test_keymap_conflict.csv");
        let mut f = std::fs::File::create(&tmp).unwrap();
        writeln!(f, "tab,key,command,description").unwrap();
        writeln!(f, "table,F,freq,Frequency").unwrap();
        writeln!(f, "table,F,from,Load file").unwrap();  // conflict!

        let result = KeyMap::load(&tmp);
        assert!(result.is_err(), "Should detect key conflict");
        assert!(result.unwrap_err().to_string().contains("Key conflict"));

        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn test_no_conflict_different_tabs() {
        let tmp = std::env::temp_dir().join("tv_test_keymap_ok.csv");
        let mut f = std::fs::File::create(&tmp).unwrap();
        writeln!(f, "tab,key,command,description").unwrap();
        writeln!(f, "table,F,freq,Frequency").unwrap();
        writeln!(f, "meta,F,filter,Filter").unwrap();  // same key, different tab = ok

        let result = KeyMap::load(&tmp);
        assert!(result.is_ok(), "Same key in different tabs should be ok");

        let _ = std::fs::remove_file(tmp);
    }
}
