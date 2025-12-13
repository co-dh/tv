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
}

/// Plugin registry - manages enabled plugins
pub struct Registry {
    plugins: Vec<Box<dyn Plugin>>,
}

impl Registry {
    /// Create registry and load enabled plugins from config
    pub fn new(cfg_path: &Path) -> Self {
        let mut enabled: HashMap<String, bool> = HashMap::new();
        if let Ok(content) = fs::read_to_string(cfg_path) {
            for line in content.lines().skip(1) {
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 2 {
                    enabled.insert(parts[0].trim().into(), parts[1].trim() == "true" || parts[1].trim() == "1");
                }
            }
        }
        let mut plugins: Vec<Box<dyn Plugin>> = vec![
            Box::new(meta::MetaPlugin), Box::new(freq::FreqPlugin),
            Box::new(folder::FolderPlugin), Box::new(system::SystemPlugin),
        ];
        plugins.retain(|p| *enabled.get(p.name()).unwrap_or(&true));
        Self { plugins }
    }

    /// Create registry with all plugins enabled
    pub fn all() -> Self {
        Self { plugins: vec![
            Box::new(meta::MetaPlugin), Box::new(freq::FreqPlugin),
            Box::new(folder::FolderPlugin), Box::new(system::SystemPlugin),
        ]}
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
        self.plugins.iter().find_map(|p| p.parse(cmd, arg))
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::all()
    }
}
