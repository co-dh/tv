#![allow(dead_code)]
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Clone)]
pub struct KeyBinding {
    pub key: String,
    pub command: String,
    pub description: String,
}

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

    /// Get hint text for a command (hardcoded mapping)
    pub fn hint(command: &str) -> Option<&'static str> {
        match command {
            "quit" => Some("quit"),
            "top" => Some("top"),
            "bottom" => Some("bottom"),
            "page_down" => Some("page down"),
            "page_up" => Some("page up"),
            "select" => Some("select"),
            "clear_sel" => Some("clear sel"),
            "decimals_inc" => Some("decimals++"),
            "decimals_dec" => Some("decimals--"),
            "load" => Some("load file"),
            "save" => Some("save file"),
            "search" => Some("search"),
            "filter" => Some("filter"),
            "next_match" => Some("next match"),
            "prev_match" => Some("prev match"),
            "freq" => Some("freq table"),
            "meta" => Some("metadata"),
            "corr" => Some("correlation"),
            "sort_asc" => Some("sort asc"),
            "sort_desc" => Some("sort desc"),
            "delete" => Some("delete"),
            "delete_sel" => Some("delete sel"),
            "filter_parent" => Some("filter parent"),
            "sel_null" => Some("sel null"),
            "sel_single" => Some("sel single"),
            "command" => Some("command"),
            _ => None,
        }
    }

    /// Get hints for info box for a tab
    pub fn get_hints(&self, tab: &str) -> Vec<(String, &'static str)> {
        let mut hints = Vec::new();
        let mut seen_cmds = std::collections::HashSet::new();

        // Collect commands to show (tab-specific first, then common)
        let tabs_to_check = vec![tab, "common"];

        for t in tabs_to_check {
            if let Some(cmds) = self.bindings.get(t) {
                for (cmd, binding) in cmds {
                    if !seen_cmds.contains(cmd) {
                        if let Some(hint) = Self::hint(cmd) {
                            hints.push((binding.key.clone(), hint));
                            seen_cmds.insert(cmd.clone());
                        }
                    }
                }
            }
        }

        // Add picker hint for table view
        if tab == "table" {
            hints.push(("Enter".to_string(), "sel+edit"));
        }

        hints
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
