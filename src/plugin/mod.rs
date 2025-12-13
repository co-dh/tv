//! Plugin architecture for view modules
//! Each plugin encapsulates: view detection, command handling, command parsing

pub mod meta;
pub mod freq;
pub mod folder;
pub mod system;

use crate::app::AppContext;
use crate::command::Command;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Plugin trait - each view type implements this
pub trait Plugin: Send + Sync {
    /// Plugin name (used in cfg/plugins.csv)
    fn name(&self) -> &str;

    /// Keymap tab name for key bindings
    fn tab(&self) -> &str;

    /// Check if a view name belongs to this plugin
    fn matches(&self, name: &str) -> bool;

    /// Handle a command for this view type
    fn handle(&self, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>>;

    /// Parse a command string into a Command (for command mode)
    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>>;

    /// List of commands this plugin provides (name, description)
    fn commands(&self) -> Vec<(&str, &str)> { vec![] }
}

/// Plugin registry - manages enabled plugins
pub struct Registry {
    plugins: Vec<Box<dyn Plugin>>,
    enabled: HashMap<String, bool>,
}

impl Registry {
    /// Create registry and load enabled plugins from config
    pub fn new(cfg_path: &Path) -> Self {
        let mut enabled = HashMap::new();

        // Load cfg/plugins.csv if exists
        if let Ok(content) = fs::read_to_string(cfg_path) {
            for line in content.lines().skip(1) {  // skip header
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 2 {
                    let name = parts[0].trim();
                    let on = parts[1].trim() == "true" || parts[1].trim() == "1";
                    enabled.insert(name.to_string(), on);
                }
            }
        }

        // Default all to enabled if not in config
        let mut plugins: Vec<Box<dyn Plugin>> = vec![
            Box::new(meta::MetaPlugin),
            Box::new(freq::FreqPlugin),
            Box::new(folder::FolderPlugin),
            Box::new(system::SystemPlugin),
        ];

        // Filter to only enabled plugins
        plugins.retain(|p| *enabled.get(p.name()).unwrap_or(&true));

        Self { plugins, enabled }
    }

    /// Create registry with all plugins enabled (no config)
    pub fn all() -> Self {
        Self {
            plugins: vec![
                Box::new(meta::MetaPlugin),
                Box::new(freq::FreqPlugin),
                Box::new(folder::FolderPlugin),
                Box::new(system::SystemPlugin),
            ],
            enabled: HashMap::new(),
        }
    }

    /// Find plugin that matches view name
    pub fn find(&self, name: &str) -> Option<&dyn Plugin> {
        self.plugins.iter().find(|p| p.matches(name)).map(|p| p.as_ref())
    }

    /// Get tab name for view
    pub fn tab(&self, name: &str) -> &str {
        self.find(name).map(|p| p.tab()).unwrap_or("table")
    }

    /// Handle command for current view
    pub fn handle(&self, view_name: &str, cmd: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
        self.find(view_name).and_then(|p| p.handle(cmd, app))
    }

    /// Parse command string
    pub fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        for plugin in &self.plugins {
            if let Some(c) = plugin.parse(cmd, arg) {
                return Some(c);
            }
        }
        None
    }

    /// List all commands from all plugins
    pub fn all_commands(&self) -> Vec<(&str, &str)> {
        self.plugins.iter().flat_map(|p| p.commands()).collect()
    }

    /// Check if plugin is enabled
    pub fn is_enabled(&self, name: &str) -> bool {
        *self.enabled.get(name).unwrap_or(&true)
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::all()
    }
}
